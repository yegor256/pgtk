# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
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
#   stash = Pgtk::Stash.new(pool, cap: 1000, refill: 30)
#   stash.start!
#   result = stash.exec('SELECT * FROM users WHERE id = $1', [42])
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
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
  # @param [Object] pool The underlying connection pool
  # @param [Hash] stash Internal cache structure
  # @param [Float] refill Interval in seconds between background refill tasks
  # @param [Float] delay A pause in seconds before making a refill
  # @param [Integer] maxqueue Maximum number of refilling tasks in the thread pool queue
  # @param [Integer] threads Number of worker threads for cache refilling
  # @param [Integer] cap Maximum number of cached query results to retain
  # @param [Float] capping Interval in seconds between cache cap enforcement tasks
  # @param [Integer] retire Maximum age in seconds to keep a query in cache
  # @param [Float] retirement Interval in seconds between retirement tasks
  # @param [Loog] loog Logger instance
  # @param [Concurrent::ReentrantReadWriteLock] entrance Read-write lock for thread-safe access
  def initialize(
    pool,
    stash: { queries: {}, tables: {}, table_mod: {} },
    loog: Loog::NULL,
    entrance: Concurrent::ReentrantReadWriteLock.new,
    refill: 16,
    delay: 0,
    maxqueue: 128,
    threads: 4,
    cap: 10_000,
    capping: 60,
    retire: 15 * 60,
    retirement: 60
  )
    @pool = pool
    @stash = stash
    @stash[:table_mod] ||= {}
    @loog = loog
    @entrance = entrance
    @refill = refill
    @delay = delay
    @maxqueue = maxqueue
    @threads = threads
    @cap = cap
    @capping = capping
    @retire = retire
    @retirement = retirement
  end

  # Start the connection pool and launch background cache management tasks.
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
  # @return [String] Multi-line text representation of the current cache state
  def dump
    @entrance.with_read_lock do
      qq = queries
      body(qq)
    end
  end

  # Execute a SQL query with optional caching.
  #
  # @param [String, Array<String>] query The SQL query
  # @param [Array] params Query parameters
  # @param [Integer] result Result format code
  # @return [PG::Result] Query result object
  def exec(query, params = [], result = 0)
    pure = (query.is_a?(Array) ? query.join(' ') : query).gsub(/\s+/, ' ').strip
    if MODS_RE.match?(pure) || /(^|\s)pg_[a-z_]+\(/.match?(pure)
      modify(pure, params, result)
    else
      select(pure, params, result)
    end
  end

  # Execute a database transaction.
  #
  # @yield [Pgtk::Stash] A stash connected to the transaction
  # @return [Object] The result of the block
  def transaction
    @pool.transaction do |t|
      yield(Pgtk::Stash.new(t, stash: @stash, loog: @loog, entrance: @entrance))
    end
  end

  private

  def queries
    @stash[:queries].map do |q, kk|
      {
        q: q.dup,
        c: kk.values.count,
        p: kk.values.sum { |vv| vv[:popularity] },
        s: kk.values.count { |vv| vv[:stale] },
        u: kk.values.map { |vv| vv[:used] }.max || Time.now
      }
    end
  end

  def body(list)
    [
      @pool.dump,
      '',
      header,
      if @tpool
        "  #{@tpool.queue_length} tasks in the thread pool"
      else
        '  Not launched yet'
      end,
      "  #{cached} queries cached (#{cached > @cap ? 'above' : 'below'} the cap)",
      "  #{@stash[:tables].count} tables in cache",
      "  #{list.sum { |a| a[:s] }} stale queries in cache:",
      stale(list),
      "  #{list.count { |a| a[:s].zero? }} other queries in cache:",
      fresh(list)
    ].join("\n")
  end

  def header
    [
      'Pgtk::Stash (',
      [
        "threads=#{@threads}",
        "maxqueue=#{@maxqueue}",
        if @refill
          [
            "refill=#{@refill}s",
            "delay=#{@delay}s"
          ]
        else
          'no refilling'
        end,
        if @capping
          [
            "capping=#{@capping}s",
            "cap=#{@cap}"
          ]
        else
          'no capping'
        end,
        if @retirement
          [
            "retirement=#{@retirement}s",
            "retire=#{@retire}"
          ]
        else
          'no retirement'
        end
      ].flatten.join(', '),
      '):'
    ].join
  end

  def stale(list)
    items = list.select { |a| a[:s].positive? }.sort_by { -_1[:p] }.take(8)
    items.map! { |a| "    #{a[:c]}/#{a[:p]}p/#{a[:s]}s/#{a[:u].ago}: #{a[:q]}" }
    items
  end

  def fresh(list)
    items = list.select { |a| a[:s].zero? }.sort_by { -_1[:p] }.take(16)
    items.map! { |a| "    #{a[:c]}/#{a[:p]}p/#{a[:s]}s/#{a[:u].ago}: #{a[:q]}" }
    items
  end

  def modify(pure, params, result)
    tables = pure.scan(ALTS_RE).flatten
    tables.uniq!
    ret = @pool.exec(pure, params, result)
    now = Time.now
    @entrance.with_write_lock do
      tables.each do |t|
        @stash[:table_mod][t] = now
        @stash[:tables][t]&.each do |q|
          @stash[:queries][q]&.each_key do |key|
            @stash[:queries][q][key][:stale] = now
          end
        end
      end
    end
    ret
  end

  def select(pure, params, result)
    key = params.join(SEPARATOR)
    ret = @stash.dig(:queries, pure, key, :ret)
    if ret.nil? || @stash.dig(:queries, pure, key, :stale)
      mark = @stash.dig(:queries, pure, key, :stale)
      tables = pure.scan(/(?<=^|\s)(?:FROM|JOIN) ([a-z_]+)(?=\s|;|$)/).flatten
      tables.uniq!
      marks = tables.to_h { |t| [t, @stash[:table_mod][t]] }
      ret = @pool.exec(pure, params, result)
      cache(pure, key, params, result, ret, mark, tables, marks) unless pure.include?(' NOW() ')
    end
    bump(pure, key) if @stash.dig(:queries, pure, key)
    ret
  end

  def cache(pure, key, params, result, ret, mark, tables, marks)
    raise(ArgumentError, "No tables at #{pure.inspect}") if tables.empty?
    @entrance.with_write_lock do
      tables.each do |t|
        @stash[:tables][t] = [] if @stash[:tables][t].nil?
        @stash[:tables][t].append(pure).uniq!
      end
      @stash[:queries][pure] ||= {}
      existing = @stash[:queries][pure][key]
      stale = existing && existing[:stale]
      stillborn = tables.any? { |t| (cur = @stash[:table_mod][t]) && cur != marks[t] }
      entry = { ret:, params:, result:, used: Time.now }
      entry[:stale] = stale == mark ? Time.now : stale if (stale && stale != mark) || stillborn
      @stash[:queries][pure][key] = entry
    end
  end

  def bump(pure, key)
    @entrance.with_write_lock do
      @stash[:queries][pure][key][:popularity] ||= 0
      @stash[:queries][pure][key][:popularity] += 1
      @stash[:queries][pure][key][:used] = Time.now
    end
  end

  # Calculate total number of cached query results.
  #
  # @return [Integer] Total count of cached query results
  def cached
    @entrance.with_write_lock do
      @stash[:queries].values.sum { |kk| kk.values.size }
    end
  end

  # Launch background tasks for cache management.
  #
  # @return [nil]
  def launch!
    @tpool = Concurrent::FixedThreadPool.new(@threads)
    capper! if @capping
    retiree! if @retirement
    refiller! if @refill
  end

  def capper!
    Concurrent::TimerTask.execute(execution_interval: @capping, executor: @tpool) do
      loop do
        break if cached <= @cap
        @entrance.with_write_lock do
          @stash[:queries].each_key do |q|
            m = @stash[:queries][q].values.map { |h| h[:used] }.min
            next unless m
            @stash[:queries][q].delete_if { |_, h| h[:used] == m }
            evict(q) if @stash[:queries][q].empty?
          end
        end
      end
    end
  end

  def retiree!
    Concurrent::TimerTask.execute(execution_interval: @retirement, executor: @tpool) do
      @entrance.with_write_lock do
        @stash[:queries].each_key do |q|
          @stash[:queries][q].delete_if { |_, h| h[:used] < Time.now - @retire }
          evict(q) if @stash[:queries][q].empty?
        end
      end
    end
  end

  def evict(query)
    @stash[:queries].delete(query)
    @stash[:tables].each_value { |list| list.delete(query) }
    @stash[:tables].delete_if { |_, list| list.empty? }
  end

  def refiller!
    Concurrent::TimerTask.execute(execution_interval: @refill, executor: @tpool) do
      ranked.each { |q| replenish(q) }
    end
  end

  def ranked
    qq =
      @entrance.with_write_lock do
        @stash[:queries]
          .map { |k, v| [k, v.values.sum { |vv| vv[:popularity] }, v.values.any? { |vv| vv[:stale] }] }
      end
    qq.select { _1[2] }.sort_by { -_1[1] }.map { _1[0] }
  end

  def replenish(query)
    snapshot =
      @entrance.with_read_lock do
        @stash[:queries][query]&.filter_map do |k, h|
          next unless h[:stale]
          next if h[:stale] > Time.now - @delay
          [k, h[:params], h[:result], h[:stale]]
        end
      end
    return unless snapshot
    snapshot.each do |k, params, result, mark|
      next if @tpool.queue_length >= @maxqueue
      @tpool.post do
        ret = @pool.exec(query, params, result)
        @entrance.with_write_lock do
          h = @stash[:queries][query]&.dig(k)
          next unless h
          if h[:stale] == mark
            h[:ret] = ret
            h.delete(:stale)
          end
        end
      end
    end
  end
end
