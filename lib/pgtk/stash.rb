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
  MODS = %w[INSERT DELETE UPDATE LOCK VACUUM TRANSACTION COMMIT ROLLBACK REINDEX TRUNCATE CREATE ALTER DROP SET].freeze
  MODS_RE = Regexp.new("(^|\\s)(#{MODS.join('|')})(\\s|$)")

  ALTS = ['UPDATE', 'INSERT INTO', 'DELETE FROM', 'TRUNCATE', 'ALTER TABLE', 'DROP TABLE'].freeze
  ALTS_RE = Regexp.new("(?<=^|\\s)(?:#{ALTS.join('|')})\\s([a-z]+)(?=[^a-z]|$)")

  private_constant :MODS, :ALTS, :MODS_RE, :ALTS_RE

  # Initialize a new Stash with query caching.
  #
  # @param [Object] pgsql PostgreSQL connection object
  # @param [Hash] stash Optional existing stash to use (default: new empty stash)
  # @param [Hash] options Optional options for internal tuning
  # @option [Integer] refresh Interval for re-calculate queries
  # @option [Integer] top Number of queries to re-calculate
  # @option [Concurrent::ReentrantReadWriteLock] entrance Lock for write internal state
  # @option [Concurrent::FixedThreadPool] threadpool ThreadPool for execution tasks in background
  # @option [Concurrent::AtomicBoolean] background Latch for start timers once
  # @option [Loog] loog Logger for debugging (default: null logger)
  def initialize(pgsql, stash = { queries: {}, tables: {} }, **options)
    @pgsql = pgsql
    @stash = stash
    @refresh = options[:refresh] || 5
    @top = options[:top] || 100
    @entrance = options[:entrance] || Concurrent::ReentrantReadWriteLock.new
    @threadpool = options[:threadpool] || Concurrent::FixedThreadPool.new(5)
    @background = options[:background] || Concurrent::AtomicBoolean.new(false)
    @loog = options[:loog] || Loog::NULL
  end

  # Execute a SQL query with optional caching.
  #
  # Read queries are cached, while write queries bypass the cache and invalidate related entries.
  #
  # @param [String, Array<String>] query The SQL query to execute
  # @param [Array] params Query parameters
  # @param [Integer] result Should be 0 for text results, 1 for binary
  # @return [PG::Result] Query result
  def exec(query, params = [], result = 0)
    pure = (query.is_a?(Array) ? query.join(' ') : query).gsub(/\s+/, ' ').strip
    if MODS_RE.match?(pure) || /(^|\s)pg_[a-z_]+\(/.match?(pure)
      tables = pure.scan(ALTS_RE).map(&:first).uniq
      ret = @pgsql.exec(pure, params, result)
      @entrance.with_write_lock do
        tables.each do |t|
          @stash[:tables][t]&.each do |q|
            @stash[:queries][q].each_key do |key|
              @stash[:queries][q][key]['stale'] = true
            end
          end
        end
      end
    else
      key = params.map(&:to_s).join(' -*&%^- ')
      @entrance.with_write_lock { @stash[:queries][pure] ||= {} }
      ret = @stash.dig(:queries, pure, key, 'ret')
      if ret.nil? || @stash.dig(:queries, pure, key, 'stale')
        ret = @pgsql.exec(pure, params, result)
        unless pure.include?(' NOW() ')
          @entrance.with_write_lock do
            @stash[:queries][pure] ||= {}
            @stash[:queries][pure][key] = { 'ret' => ret, 'params' => params, 'result' => result }
            tables = pure.scan(/(?<=^|\s)(?:FROM|JOIN) ([a-z_]+)(?=\s|$)/).map(&:first).uniq
            tables.each do |t|
              @stash[:tables][t] = [] if @stash[:tables][t].nil?
              @stash[:tables][t].append(pure).uniq!
            end
            raise "No tables at #{pure.inspect}" if tables.empty?
          end
        end
      end
      count(pure, key)
    end
    background
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
      yield Pgtk::Stash.new(
        t, @stash,
        refresh: @refresh,
        top: @top,
        entrance: @entrance,
        threadpool: @threadpool,
        background: @background,
        loog: @loog
      )
    end
  end

  # Start a new connection pool with the given arguments.
  #
  # @param args Arguments to pass to the underlying pool's start method
  # @return [Pgtk::Stash] A new stash that shares the same cache
  def start(*args)
    Pgtk::Stash.new(
      @pgsql.start(*args), @stash,
      refresh: @refresh,
      top: @top,
      entrance: @entrance,
      threadpool: @threadpool,
      background: @background,
      loog: @loog
    )
  end

  # Get the PostgreSQL server version.
  #
  # @return [String] Version string of the database server
  def version
    @pgsql.version
  end

  # Get statistics on the most used queries
  #
  # @return [Array<Array<String, Integer>>] Array of query and hits in desc hits order
  def stats
    @stash[:queries].map { |k, v| [k.dup, v.values.sum { |vv| vv['count'] }] }.sort_by { -_1[1] }
  end

  private

  def count(query, key)
    @entrance.with_write_lock do
      @stash[:queries][query][key]['count'] ||= 0
      @stash[:queries][query][key]['count'] += 1
    end
  end

  def background
    return unless @background.make_true
    Concurrent::TimerTask.execute(execution_interval: 24 * 60 * 60, executor: @threadpool) do
      @entrance.with_write_lock do
        @stash[:queries].each_key do |q|
          @stash[:queries][q].each_key do |k|
            @stash[:queries][q][k]['count'] = 0
          end
        end
      end
    end
    Concurrent::TimerTask.execute(execution_interval: @refresh, executor: @threadpool) do
      @stash[:queries]
        .map { |k, v| [k, v.values.sum { |vv| vv['count'] }] }
        .sort_by { -_1[1] }
        .first(@top)
        .each do |a|
        q = a[0]
        @stash[:queries][q].each_key do |k|
          next unless @stash[:queries][q][k]['stale']
          @threadpool.post do
            params = @stash[:queries][q][k]['params']
            result = @stash[:queries][q][k]['result']
            ret = @pgsql.exec(q, params, result)
            @entrance.with_write_lock do
              @stash[:queries][q] ||= {}
              @stash[:queries][q][k] = { 'ret' => ret, 'params' => params, 'result' => result, 'count' => 1 }
            end
          end
        end
      end
    end
    nil
  end
end
