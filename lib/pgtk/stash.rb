# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'concurrent-ruby'
require 'joined'
require 'loog'
require_relative '../pgtk'

# Database query cache implementation.
#
# Provides a caching layer for PostgreSQL queries, automatically invalidating
# the cache when tables are modified. Read queries are cached while write
# queries bypass the cache and invalidate related cached entries.
#
# Thread-safe with read-write locking.
#
# The implementation is very naive! Use it at your own risk.
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2025 Yegor Bugayenko
# License:: MIT
class Pgtk::Stash
  # Initialize a new Stash with query caching.
  #
  # @param [Object] pgsql PostgreSQL connection object
  # @param [Hash] stash Optional existing stash to use (default: new empty stash)
  # @param [Loog] loog Logger for debugging (default: null logger)
  def initialize(pgsql, stash = {})
    @pgsql = pgsql
    @stash = stash
    @stash[:queries] ||= {}
    @stash[:tables] ||= {}
    @entrance = Concurrent::ReentrantReadWriteLock.new
  end

  # Execute a SQL query with optional caching.
  #
  # Read queries are cached, while write queries bypass the cache and invalidate related entries.
  #
  # @param [String, Array<String>] query The SQL query to execute
  # @param [Array] params Query parameters
  # @return [PG::Result] Query result
  def exec(query, params = [])
    pure = (query.is_a?(Array) ? query.join(' ') : query).gsub(/\s+/, ' ').strip
    if /(^|\s)(INSERT|DELETE|UPDATE|LOCK)\s/.match?(pure) || /(^|\s)pg_[a-z_]+\(/.match?(pure)
      tables = pure.scan(/(?<=^|\s)(?:UPDATE|INSERT INTO|DELETE FROM|TRUNCATE)\s([a-z]+)(?=[^a-z]|$)/).map(&:first).uniq
      ret = @pgsql.exec(pure, params)
      @entrance.with_write_lock do
        tables.each do |t|
          @stash[:tables][t]&.each do |q|
            @stash[:queries].delete(q)
          end
          @stash[:tables].delete(t)
        end
      end
    else
      key = params.map(&:to_s).join(' -*&%^- ')
      @entrance.with_write_lock { @stash[:queries][pure] ||= {} }
      ret = @stash[:queries][pure][key]
      if ret.nil?
        ret = @pgsql.exec(pure, params)
        if pure.start_with?('SELECT ')
          @entrance.with_write_lock do
            @stash[:queries][pure] ||= {}
            @stash[:queries][pure][key] = ret
            tables = pure.scan(/(?<=^|\s)(?:FROM|JOIN) ([a-z_]+)(?=\s|$)/).map(&:first).uniq
            tables.each do |t|
              @stash[:tables][t] = [] if @stash[:tables][t].nil?
              @stash[:tables][t].append(pure).uniq!
            end
            raise "No tables at #{pure.inspect}" if tables.empty?
          end
        end
      end
    end
    ret
  end

  # Execute a database transaction.
  #
  # Yields a new Stash that shares the same cache but uses the transaction connection.
  #
  # @yield [Pgtk::Stash] A stash connected to the transaction
  # @return [Object] The result of the block
  def transaction
    @pgsql.transaction do |t|
      yield Pgtk::Stash.new(t, @stash)
    end
  end

  # Start a new connection pool with the given arguments.
  #
  # @param args Arguments to pass to the underlying pool's start method
  # @return [Pgtk::Stash] A new stash that shares the same cache
  def start(*args)
    Pgtk::Stash.new(@pgsql.start(*args), @stash)
  end

  # Get the PostgreSQL server version.
  #
  # @return [String] Version string of the database server
  def version
    @pgsql.version
  end
end
