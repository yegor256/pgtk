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
  # @param [Integer] refill_interval Interval in seconds for recalculate stale queries
  # @param [Integer] cap How many queries to keep in cache (if more, oldest ones are deleted)
  # @param [Integer] cap_interval Interval in seconds for cap the cache (remove old queries)
  # @param [Integer] max_queue_length Number of refilling tasks in the queue
  # @param [Integer] threads Number of threads in tpool
  # @param [Loog] loog Logger for debugging (default: null logger)
  def initialize(
    pool,
    stash: { queries: {}, tables: {} },
    refill_interval: 16,
    max_queue_length: 128,
    threads: 4,
    cap: 10_000,
    cap_interval: 60,
    loog: Loog::NULL,
    entrance: Concurrent::ReentrantReadWriteLock.new,
    launched: Concurrent::AtomicBoolean.new(false)
  )
    @pool = pool
    @stash = stash
    @launched = launched
    @entrance = entrance
    @refill_interval = refill_interval
    @max_queue_length = max_queue_length
    @threads = threads
    @cap = cap
    @cap_interval = cap_interval
    @loog = loog
    @tpool = Concurrent::FixedThreadPool.new(@threads)
  end

  # Start a new connection pool with the given arguments.
  def start!
    launch!
    @pool.start!
  end

  # Get the PostgreSQL server version.
  # @return [String] Version string of the database server
  def version
    @pool.version
  end

  # Convert internal state into text.
  def dump
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
      # rubocop:disable Layout/LineLength
      "Pgtk::Stash (refill_interval=#{@refill_interval}s, max_queue_length=#{@max_queue_length}, threads=#{@threads}, cap=#{@cap}, cap_interval=#{@cap_interval}):",
      # rubocop:enable Layout/LineLength
      "  #{'not ' if @launched.false?}launched",
      "  #{@tpool.queue_length} task(s) in the thread pool",
      "  #{@stash[:tables].count} table(s) in cache",
      "  #{qq.sum { |a| a[:s] }} stale quer(ies) in cache:",
      qq.select { |a| a[:s].positive? }.sort_by { -_1[:p] }.take(8).map do |a|
        "    #{a[:c]}/#{a[:p]}p/#{a[:s]}s/#{a[:u].ago}: #{a[:q]}"
      end,
      "  #{qq.count { |a| a[:s].zero? }} other quer(ies) in cache:",
      qq.select { |a| a[:s].zero? }.sort_by { -_1[:p] }.take(16).map do |a|
        "    #{a[:c]}/#{a[:p]}p/#{a[:s]}s/#{a[:u].ago}: #{a[:q]}"
      end
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
          tables = pure.scan(/(?<=^|\s)(?:FROM|JOIN) ([a-z_]+)(?=\s|$)/).map(&:first).uniq
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
        refill_interval: @refill_interval,
        max_queue_length: @max_queue_length,
        threads: @threads,
        loog: @loog,
        entrance: @entrance,
        launched: @launched
      )
    end
  end

  private

  def launch!
    raise 'Cannot launch multiple times on same cache data' unless @launched.make_true
    Concurrent::TimerTask.execute(execution_interval: @cap_interval, executor: @tpool) do
      loop do
        s = @stash[:queries].values.sum { |kk| kk.values.size }
        break if s <= @cap
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
    Concurrent::TimerTask.execute(execution_interval: @refill_interval, executor: @tpool) do
      @stash[:queries]
        .map { |k, v| [k, v.values.sum { |vv| vv[:popularity] }, v.values.any? { |vv| vv[:stale] }] }
        .select { _1[2] }
        .sort_by { -_1[1] }
        .each do |a|
        q = a[0]
        @stash[:queries][q].each_key do |k|
          next unless @stash[:queries][q][k][:stale]
          next if @tpool.queue_length >= @max_queue_length
          @tpool.post do
            h = @stash[:queries][q][k]
            ret = @pool.exec(q, h[:params], h[:result])
            @entrance.with_write_lock do
              h = @stash[:queries][q][k]
              h[:stale] = false
              h[:ret] = ret
            end
          end
        end
      end
    end
    nil
  end
end
