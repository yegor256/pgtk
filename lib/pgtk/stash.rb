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

  SEPARATOR = ' --%*@#~($-- '

  private_constant :MODS, :ALTS, :MODS_RE, :ALTS_RE, :SEPARATOR

  # Initialize a new Stash with query caching.
  #
  # @param [Object] pool Original object
  # @param [Hash] stash Optional existing stash to use (default: new empty stash)
  # @option [Hash] queries Internal cache data (default: {})
  # @option [Hash] tables Internal cache data (default: {})
  # @option [Concurrent::ReentrantReadWriteLock] entrance Lock for write internal state
  # @option [Concurrent::AtomicBoolean] launched Latch for start timers once
  # @param [Integer] refill_interval Interval in seconds for recalculate stale queries
  # @param [Integer] top Number of queries to recalculate
  # @param [Integer] threads Number of threads in threadpool
  # @param [Loog] loog Logger for debugging (default: null logger)
  def initialize(
    pool,
    stash = {
      queries: {},
      tables: {},
      entrance: Concurrent::ReentrantReadWriteLock.new,
      launched: Concurrent::AtomicBoolean.new(false)
    },
    refill_interval: 5,
    top: 100,
    threads: 5,
    loog: Loog::NULL
  )
    @pool = pool
    @stash = stash
    @entrance = stash[:entrance]
    @refill_interval = refill_interval
    @top = top
    @threads = threads
    @loog = loog
  end

  # Get the PostgreSQL server version.
  # @return [String] Version string of the database server
  def version
    @pool.version
  end

  # Convert internal state into text.
  def dump
    qq =
      @stash[:queries].map do |k, v|
        [
          k.dup, # the query
          v.values.count, # how many keys?
          v.values.sum { |vv| vv[:popularity] }, # total popularity of all keys
          v.values.count { |vv| vv[:stale] } # how many stale keys?
        ]
      end
    [
      @pool.dump,
      '',
      "Pgtk::Stash (refill_interval=#{@refill_interval}s, top=#{@top}q, threads=#{@threads}t):",
      "  #{'not ' if @stash[:launched].false?}launched",
      "  #{@stash[:tables].count} tables in cache",
      "  #{@stash[:queries].count} queries in cache:",
      qq.sort_by { -_1[2] }.take(20).map { |a| "    #{a[1]}/#{a[2]}p/#{a[3]}s: #{a[0]}" }
    ].join("\n")
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
      ret = @pool.exec(pure, params, result)
      @entrance.with_write_lock do
        tables.each do |t|
          @stash[:tables][t]&.each do |q|
            @stash[:queries][q].each_key do |key|
              @stash[:queries][q][key][:stale] = true
            end
          end
        end
      end
    else
      key = params.map(&:to_s).join(SEPARATOR)
      ret = @stash.dig(:queries, pure, key, :ret)
      if ret.nil? || @stash.dig(:queries, pure, key, :stale)
        ret = @pool.exec(pure, params, result)
        unless pure.include?(' NOW() ')
          @entrance.with_write_lock do
            tables = pure.scan(/(?<=^|\s)(?:FROM|JOIN) ([a-z_]+)(?=\s|$)/).map(&:first).uniq
            raise "No tables at #{pure.inspect}" if tables.empty?
            @stash[:queries][pure] ||= {}
            @stash[:queries][pure][key] = { ret:, params:, result: }
            tables.each do |t|
              @stash[:tables][t] = [] if @stash[:tables][t].nil?
              @stash[:tables][t].append(pure).uniq!
            end
          end
        end
      end
      if @stash.dig(:queries, query, key)
        @entrance.with_write_lock do
          @stash[:queries][query][key][:popularity] ||= 0
          @stash[:queries][query][key][:popularity] += 1
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
    @pool.transaction do |t|
      yield Pgtk::Stash.new(
        t, @stash,
        refill_interval: @refill_interval,
        top: @top,
        threads: @threads,
        loog: @loog
      )
    end
  end

  # Start a new connection pool with the given arguments.
  # @return [Pgtk::Stash] A new stash that shares the same cache
  def start(*)
    launch!
    Pgtk::Stash.new(
      @pool.start(*), @stash,
      refill_interval: @refill_interval,
      top: @top,
      threads: @threads,
      loog: @loog
    )
  end

  private

  def launch!
    raise 'Cannot launch multiple times on same cache data' unless @stash[:launched].make_true
    Concurrent::FixedThreadPool.new(@threads).then do |threadpool|
      Concurrent::TimerTask.execute(execution_interval: 60 * 60, executor: threadpool) do
        @entrance.with_write_lock do
          @stash[:queries].each_key do |q|
            @stash[:queries][q].each_key do |k|
              @stash[:queries][q][k][:popularity] = 0
            end
          end
        end
      end
      Concurrent::TimerTask.execute(execution_interval: @refill_interval, executor: threadpool) do
        @stash[:queries]
          .map { |k, v| [k, v.values.sum { |vv| vv[:popularity] }, v.values.any? { |vv| vv[:stale] }] }
          .select { _1[2] }
          .sort_by { -_1[1] }
          .first(@top)
          .each do |a|
          q = a[0]
          @stash[:queries][q].each_key do |k|
            next unless @stash[:queries][q][k][:stale]
            threadpool.post do
              @entrance.with_write_lock do
                h = @stash[:queries][q][k]
                h[:stale] = false
                h[:ret] = @pool.exec(q, h[:params], h[:result])
              end
            end
          end
        end
      end
    end
    nil
  end
end
