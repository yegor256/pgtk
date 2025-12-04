# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'concurrent-ruby'
require 'joined'
require 'loog'
require 'tago'
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
# @example Basic usage
#   pool = Pgtk::Pool.new(...)
#   stash = Pgtk::Stash.new(pool, cap: 1000, refill_interval: 30)
#   stash.start!
#   result = stash.exec('SELECT * FROM users WHERE id = $1', [42])
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
  # Set any of the intervals to nil to disable the cron.
  #
  # @param [Object] pool The underlying connection pool that executes actual database queries
  # @param [Hash] stash Internal cache structure containing queries and tables hashes for sharing state
  #   across transactions
  # @param [Float] refill_interval Interval in seconds between background tasks that recalculate stale
  #   cached queries
  # @param [Float] refill_delay A pause in seconds we take before making a refill
  # @param [Integer] max_queue_length Maximum number of refilling tasks allowed in the thread pool queue
  #   before new tasks are skipped
  # @param [Integer] threads Number of worker threads in the background thread pool for cache refilling
  #   operations
  # @param [Integer] cap Maximum number of cached query results to retain; oldest queries are evicted when
  #   this limit is exceeded
  # @param [Float] cap_interval Interval in seconds between background tasks that enforce the cache size
  #   cap by removing old queries
  # @param [Integer] retire Maximum age in seconds to keep a query in cache after its latest usage
  # @param [Float] retire_interval Interval in seconds between background tasks that remove
  #   retired queries
  # @param [Loog] loog Logger instance for debugging and monitoring cache operations (default: null logger)
  # @param [Concurrent::ReentrantReadWriteLock] entrance Read-write lock for thread-safe cache access
  #   shared across instances
  def initialize(
    pool,
    stash: { queries: {}, tables: {} },
    loog: Loog::NULL,
    entrance: Concurrent::ReentrantReadWriteLock.new,
    refill_interval: 16,
    refill_delay: 0,
    max_queue_length: 128,
    threads: 4,
    cap: 10_000,
    cap_interval: 60,
    retire: 15 * 60,
    retire_interval: 60
  )
    @pool = pool
    @stash = stash
    @loog = loog
    @entrance = entrance
    @refill_interval = refill_interval
    @refill_delay = refill_delay
    @max_queue_length = max_queue_length
    @threads = threads
    @cap = cap
    @cap_interval = cap_interval
    @retire = retire
    @retire_interval = retire_interval
  end

  # Start the connection pool and launch background cache management tasks.
  #
  # Initializes background timer tasks for cache refilling and size capping.
  # The refill task periodically updates stale cached queries based on popularity.
  # The cap task removes oldest queries when cache size exceeds the configured limit.
  #
  # @return [void]
  def start!
    @pool.start!
    launch!
  end

  # Get the PostgreSQL server version.
  # @return [String] Version string of the database server
  def version
    @pool.version
  end

  # Convert internal state into text.
  #
  # Generates a detailed report of the cache state including query counts,
  # popularity scores, stale queries, and thread pool status.
  #
  # @return [String] Multi-line text representation of the current cache state
  def dump
    @entrance.with_read_lock do
      qq =
        @stash[:queries].map do |q, kk|
          {
            q: q.dup, # the query
            c: kk.values.count, # how many keys?
            p: kk.values.sum { |vv| vv[:popularity] }, # total popularity of all keys
            s: kk.values.count { |vv| vv[:stale] }, # how many stale keys?
            u: kk.values.map { |vv| vv[:used] }.max || Time.now # when was it used
          }
        end
      [
        @pool.dump,
        '',
        [
          'Pgtk::Stash (',
          [
            "threads=#{@threads}",
            "max_queue_length=#{@max_queue_length}",
            if @refill_interval
              [
                "refill_interval=#{@refill_interval}s",
                "refill_delay=#{@refill_delay}s"
              ]
            else
              'no refilling'
            end,
            if @cap_interval
              [
                "cap_interval=#{@cap_interval}s",
                "cap=#{@cap}"
              ]
            else
              'no capping'
            end,
            if @retire_interval
              [
                "retire_interval=#{@retire_interval}s",
                "retire=#{@retire}"
              ]
            else
              'no retirement'
            end
          ].flatten.join(', '),
          '):'
        ].join,
        if @tpool
          "  #{@tpool.queue_length} tasks in the thread pool"
        else
          '  Not launched yet'
        end,
        "  #{stash_size} queries cached (#{stash_size > @cap ? 'above' : 'below'} the cap)",
        "  #{@stash[:tables].count} tables in cache",
        "  #{qq.sum { |a| a[:s] }} stale queries in cache:",
        qq.select { |a| a[:s].positive? }.sort_by { -_1[:p] }.take(8).map do |a|
          "    #{a[:c]}/#{a[:p]}p/#{a[:s]}s/#{a[:u].ago}: #{a[:q]}"
        end,
        "  #{qq.count { |a| a[:s].zero? }} other queries in cache:",
        qq.select { |a| a[:s].zero? }.sort_by { -_1[:p] }.take(16).map do |a|
          "    #{a[:c]}/#{a[:p]}p/#{a[:s]}s/#{a[:u].ago}: #{a[:q]}"
        end
      ].join("\n")
    end
  end

  # Execute a SQL query with optional caching.
  #
  # Read queries are cached, while write queries bypass the cache and invalidate related entries.
  # Queries containing modification keywords (INSERT, UPDATE, DELETE, etc.) are executed directly
  # and trigger invalidation of cached queries for affected tables. Read queries (SELECT)
  # are cached by query text and parameter values. Queries containing NOW() are never cached.
  #
  # @param [String, Array<String>] query The SQL query to execute as a string or array of strings to be joined
  # @param [Array] params Query parameters for placeholder substitution in prepared statements (default: empty array)
  # @param [Integer] result Result format code where 0 requests text format and 1 requests binary format (default: 0)
  # @return [PG::Result] Query result object containing rows and metadata from the database
  def exec(query, params = [], result = 0)
    pure = (query.is_a?(Array) ? query.join(' ') : query).gsub(/\s+/, ' ').strip
    if MODS_RE.match?(pure) || /(^|\s)pg_[a-z_]+\(/.match?(pure)
      tables = pure.scan(ALTS_RE).map(&:first).uniq
      ret = @pool.exec(pure, params, result)
      @entrance.with_write_lock do
        tables.each do |t|
          @stash[:tables][t]&.each do |q|
            @stash[:queries][q]&.each_key do |key|
              @stash[:queries][q][key][:stale] = Time.now
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
          tables = pure.scan(/(?<=^|\s)(?:FROM|JOIN) ([a-z_]+)(?=\s|;|$)/).map(&:first).uniq
          raise "No tables at #{pure.inspect}" if tables.empty?
          @entrance.with_write_lock do
            tables.each do |t|
              @stash[:tables][t] = [] if @stash[:tables][t].nil?
              @stash[:tables][t].append(pure).uniq!
            end
            @stash[:queries][pure] ||= {}
            @stash[:queries][pure][key] = { ret:, params:, result:, used: Time.now }
          end
        end
      end
      if @stash.dig(:queries, pure, key)
        @entrance.with_write_lock do
          @stash[:queries][pure][key][:popularity] ||= 0
          @stash[:queries][pure][key][:popularity] += 1
          @stash[:queries][pure][key][:used] = Time.now
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
        t,
        stash: @stash,
        loog: @loog,
        entrance: @entrance
      )
    end
  end

  private

  # Calculate total number of cached query results.
  #
  # Counts all cached query-parameter combinations across all queries.
  #
  # @return [Integer] Total count of cached query results
  def stash_size
    @entrance.with_write_lock do
      @stash[:queries].values.sum { |kk| kk.values.size }
    end
  end

  # Launch background tasks for cache management.
  #
  # Starts two concurrent timer tasks: one for enforcing cache size cap by removing
  # oldest queries, and another for refilling stale cached queries based on popularity.
  # This method can only be called once per cache instance.
  #
  # @return [nil]
  # @raise [RuntimeError] if background tasks have already been launched on this cache instance
  def launch!
    @tpool = Concurrent::FixedThreadPool.new(@threads)
    if @cap_interval
      Concurrent::TimerTask.execute(execution_interval: @cap_interval, executor: @tpool) do
        loop do
          break if stash_size <= @cap
          @entrance.with_write_lock do
            @stash[:queries].each_key do |q|
              m = @stash[:queries][q].values.map { |h| h[:used] }.min
              next unless m
              @stash[:queries][q].delete_if { |_, h| h[:used] == m }
              @stash[:queries].delete_if { |_, kk| kk.empty? }
            end
          end
        end
      end
    end
    if @retire_interval
      Concurrent::TimerTask.execute(execution_interval: @retire_interval, executor: @tpool) do
        @entrance.with_write_lock do
          @stash[:queries].each_key do |q|
            @stash[:queries][q].delete_if { |_, h| h[:used] < Time.now - @retire }
            @stash[:queries].delete_if { |_, kk| kk.empty? }
          end
        end
      end
    end
    return unless @refill_interval
    Concurrent::TimerTask.execute(execution_interval: @refill_interval, executor: @tpool) do
      qq =
        @entrance.with_write_lock do
          @stash[:queries]
            .map { |k, v| [k, v.values.sum { |vv| vv[:popularity] }, v.values.any? { |vv| vv[:stale] }] }
        end
      qq =
        qq.select { _1[2] }
          .sort_by { -_1[1] }
          .map { _1[0] }
      qq.each do |q|
        @entrance.with_write_lock { @stash[:queries][q].keys }.each do |k|
          next unless @stash[:queries][q][k][:stale]
          next if @stash[:queries][q][k][:stale] > Time.now - @refill_delay
          next if @tpool.queue_length >= @max_queue_length
          @tpool.post do
            h = @stash[:queries][q][k]
            ret = @pool.exec(q, h[:params], h[:result])
            @entrance.with_write_lock do
              h = @stash[:queries][q][k]
              h.delete(:stale)
              h[:ret] = ret
            end
          end
        end
      end
    end
  end
end
