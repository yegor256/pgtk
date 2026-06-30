# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'ellipsized'
require 'loog'
require 'pg'
require 'tago'
require_relative '../pgtk'
require_relative 'version'
require_relative 'wire'

# Pool provides a connection pool for PostgreSQL database connections.
# It manages a fixed number of connections to optimize performance and
# resource usage while providing a simple interface for database operations.
#
# The Pool class handles connection lifecycle, reconnects on errors,
# and provides transaction support. It's the core class for interacting
# with a PostgreSQL database in this library.
#
# Basic usage:
#
#   # Create a wire that knows how to connect to PostgreSQL
#   wire = Pgtk::Wire::Direct.new(
#     host: 'localhost',
#     port: 5432,
#     dbname: 'mydatabase',
#     user: 'postgres',
#     password: 'secret'
#   )
#
#   # Create and start a connection pool with 4 connections
#   pool = Pgtk::Pool.new(wire, max: 4)
#   pool.start!
#
#   # Execute a simple query
#   pool.exec('SELECT * FROM users')
#
#   # Execute a parameterized query
#   pool.exec('SELECT * FROM users WHERE email = $1', ['user@example.com'])
#
#   # Use transactions for multiple operations
#   pool.transaction do |t|
#     t.exec('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [100, 42])
#     t.exec('UPDATE accounts SET balance = balance + $1 WHERE id = $2', [100, 43])
#   end
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class Pgtk::Pool
  # Constructor.
  #
  # The +idle+ option guards against the cold-slot SSL desync that bites
  # managed PostgreSQL behind a TLS proxy: a slot sits idle long enough for
  # the proxy and the client to disagree about SSL state, libpq still reports
  # +CONNECTION_OK+, and the next real query blows up with a decryption error.
  # When a slot has been idle longer than +idle+ seconds, the pool runs
  # +SELECT 1+ on it before yielding; if that fails, the slot is renewed
  # in-line and the caller never sees the error. Set to +nil+ to skip
  # validation entirely (e.g. for local Unix-socket PostgreSQL).
  #
  # @param [Pgtk::Wire] wire The wire
  # @param [Integer] max Total amount of PostgreSQL connections in the pool
  # @param [Numeric] timeout Max seconds to wait for a free connection
  # @param [Numeric, nil] idle Seconds of idleness after which to validate
  #   a connection on checkout, or +nil+ to disable validation
  # @param [Object] log The log
  def initialize(wire, max: 8, timeout: 1, idle: 60, log: Loog::NULL)
    @wire = wire
    @max = max
    @idle = idle
    @log = log
    @pool = IterableQueue.new(max, timeout)
    @lock = Mutex.new
    @started = false
  end

  # Get the version of PostgreSQL server.
  #
  # @return [String] Version of PostgreSQL server
  def version
    @version ||=
      begin
        conn = @pool.pop
        @pool.push(conn)
        conn.parameter_status('server_version').split[0]
      end
  end

  # Get as much details about it as possible.
  #
  # @return [String] Summary of inner state
  def dump
    [
      'Pgtk::Pool',
      "  Pgtk version: #{Pgtk::VERSION}",
      "  PgSQL version: #{version}",
      "  #{@pool.size} connections:",
      @pool.map { |conn| info(conn) }
    ].flatten.join("\n")
  end

  # Start it with a fixed number of connections. The amount of connections
  # is specified in +max+ argument and should be big enough to handle
  # the amount of parallel connections you may have to the database. However,
  # keep in mind that not all servers will allow you to have many connections
  # open at the same time. For example, Heroku free PostgreSQL database
  # allows only one connection open.
  def start!
    @lock.synchronize do
      return if @started
      @max.times do
        @pool.push(@wire.connection)
      end
      (2 * @max).times do
        connect { |c| c.exec('SELECT 1') }
      rescue StandardError => e
        @log.warn("Pool warm-up query failed, slot will be retried: #{e.message.strip}")
      end
      @max.times do
        connect { |c| c.exec('SELECT 1') }
      end
      @started = true
      @log.debug("PostgreSQL pool started with #{@max} connections")
    end
  end

  # Make a query and return the result as an array of hashes. For example,
  # in order to fetch the list of all books belonging to the user:
  #
  #  books = pool.exec('SELECT * FROM book WHERE owner = $1', ['yegor256'])
  #  books.each do |row|
  #    puts 'ID: ' + row['id'].to_i
  #    puts 'Created: ' + Time.parse(row['created'])
  #    puts 'Title: ' + row['title']
  #  end
  #
  # All values in the retrieved hash are strings. No matter what types
  # of data you have in the database, you get strings here. It's your job
  # to convert them to the type you need.
  #
  # In order to insert a new row (pay attention to the +RETURNING+ clause
  # at the end of the SQL query):
  #
  #  id = pool.exec(
  #    'INSERT INTO book (owner, title) VALUES ($1, $2) RETURNING id',
  #    ['yegor256', 'Elegant Objects']
  #  )[0]['id'].to_i
  #
  # You can also pass a block to this method, if you want to get an instance
  # of +PG::Result+ instead of an array of hashes:
  #
  #  pool.exec('SELECT * FROM book WHERE owner = $1', ['yegor256']) do |res|
  #    res.each do |row|
  #      puts 'ID: ' + row['id'].to_i
  #      puts 'Title: ' + row['title']
  #    end
  #  end
  #
  # When the query is too long it's convenient to use an array to specify it:
  #
  #  pool.exec(
  #    [
  #      'SELECT * FROM book',
  #      'LEFT JOIN user ON user.id = book.owner',
  #      'WHERE user.login = $1 AND book.title = $2'
  #    ],
  #    ['yegor256', 'Elegant Objects']
  #  )
  #
  # More details about +exec_params+, which is called here, you can find
  # here: https://www.rubydoc.info/gems/pg/0.17.1/PG%2FConnection:exec_params
  #
  # @param [String] query The SQL query with params inside (possibly)
  # @param [Array] args List of arguments
  # @param [Integer] result Should be 0 for text results, 1 for binary
  # @yield [Hash] Rows
  def exec(query, args = [], result = 0, &block)
    connect do |c|
      t = Txn.new(c, @log)
      if block_given?
        t.exec(query, args, result, &block)
      else
        t.exec(query, args, result)
      end
    end
  end

  # Run a transaction. The block has to be provided. It will receive
  # a temporary object, which implements method +exec+, which works
  # exactly like the method +exec+ of class +Pool+, for example:
  #
  #  pgsql.transaction do |t|
  #    t.exec('DELETE FROM user WHERE id = $1', [id])
  #    t.exec('INSERT INTO user (name) VALUES ($1)', [name])
  #  end
  def transaction
    connect do |c|
      t = Txn.new(c, @log)
      t.exec('START TRANSACTION')
      begin
        yield(t).tap { t.exec('COMMIT') }
      ensure
        if c.transaction_status != PG::Constants::PQTRANS_IDLE
          begin
            t.exec('ROLLBACK')
          rescue StandardError => e
            @log.warn("Failed to rollback transaction: #{e.message}")
          end
        end
      end
    end
  end

  # Grab a single connection from the pool and yield an executor bound to it,
  # WITHOUT starting a transaction. Unlike +transaction+, no +START TRANSACTION+
  # is issued, which makes this suitable for statements that PostgreSQL refuses
  # to run inside a transaction block, such as +VACUUM+, +REINDEX+, or
  # +CREATE INDEX CONCURRENTLY+. All statements executed through the yielded
  # object run on the same connection, so a session-level setting (for example
  # +SET statement_timeout+) made earlier in the block stays in effect for the
  # statements that follow:
  #
  #  pgsql.session do |s|
  #    s.exec('SET statement_timeout = 5000')
  #    s.exec('VACUUM book')
  #    s.exec('RESET statement_timeout')
  #  end
  #
  # @yield [Object] Yields an executor that responds to +exec+
  # @return [Object] Result of the block
  def session
    connect do |c|
      yield(Txn.new(c, @log))
    end
  end

  private

  def connect
    conn = @pool.pop
    begin
      reason = cause(conn) || stale(conn)
      if reason
        begin
          conn = renew(conn, reason)
        rescue StandardError => e
          @log.warn("Failed to renew dead connection (#{reason}): #{e.message}")
          raise(e)
        end
      end
      begin
        yield(conn)
      rescue StandardError => e
        begin
          conn = renew(conn, "query failed: #{e.message.strip}")
        rescue StandardError => re
          @log.warn("Failed to renew connection after #{e.message}: #{re.message}")
        end
        raise(e)
      end
    ensure
      conn.instance_variable_set(:@pgtk_last_used, Time.now) if @idle && !conn.finished?
      @pool.push(conn)
    end
  end

  def cause(conn)
    return 'finished' if conn.finished?
    return 'status BAD' if conn.status == PG::Constants::CONNECTION_BAD
    return "transaction status #{conn.transaction_status}" if conn.transaction_status != PG::Constants::PQTRANS_IDLE
    nil
  rescue StandardError => e
    "inspection failed: #{e.message.strip}"
  end

  def stale(conn)
    return if @idle.nil?
    last = conn.instance_variable_get(:@pgtk_last_used)
    return if last.nil? || Time.now - last < @idle
    begin
      conn.exec('SELECT 1')
      nil
    rescue StandardError => e
      "validation failed after #{last.ago} idle: #{e.message.strip}"
    end
  end

  def info(conn)
    conn.instance_variable_set(:@pgtk_pid, conn.backend_pid)
    parts = [
      '    ',
      "##{conn.backend_pid}",
      {
        PG::Constants::PQ_PIPELINE_ON => 'ON', PG::Constants::PQ_PIPELINE_OFF => 'OFF',
        PG::Constants::PQ_PIPELINE_ABORTED => 'ABORTED'
      }.fetch(conn.pipeline_status, "pipeline_status=#{conn.pipeline_status}"),
      { PG::Constants::CONNECTION_OK => 'OK', PG::Constants::CONNECTION_BAD => 'BAD' }.fetch(
        conn.status,
        "status=#{conn.status}"
      ),
      {
        PG::Constants::PQTRANS_IDLE => 'IDLE', PG::Constants::PQTRANS_ACTIVE => 'ACTIVE',
        PG::Constants::PQTRANS_INTRANS => 'INTRANS', PG::Constants::PQTRANS_INERROR => 'INERROR',
        PG::Constants::PQTRANS_UNKNOWN => 'UNKNOWN'
      }.fetch(conn.transaction_status, "transaction_status=#{conn.transaction_status}")
    ]
    if conn.transaction_status != PG::Constants::PQTRANS_IDLE
      started = conn.instance_variable_get(:@pgtk_started_at)
      parts << started.ago if started
    end
    if conn.transaction_status == PG::Constants::PQTRANS_ACTIVE
      running = conn.instance_variable_get(:@pgtk_last_query)
      parts << "running: #{running.gsub(/\s+/, ' ').strip.ellipsized(60)}" if running
    end
    parts.join(' ')
  rescue PG::ConnectionBad => e
    pid = conn.instance_variable_get(:@pgtk_pid)
    parts = ['    ']
    parts << (pid ? "##{pid}" : '#?')
    parts << e.message.gsub(/\s+/, ' ').strip
    closed = conn.instance_variable_get(:@pgtk_closed_at)
    parts << "#{closed.ago} ago" if closed
    reason = conn.instance_variable_get(:@pgtk_closed_reason)
    parts << "because: #{reason.gsub(/\s+/, ' ').strip}" if reason
    last = conn.instance_variable_get(:@pgtk_last_query)
    parts << "last query: #{last.gsub(/\s+/, ' ').strip.ellipsized(60)}" if last
    "#{parts.shift} #{parts.shift} #{parts.join(', ')}"
  end

  def renew(conn, reason)
    begin
      unless conn.finished?
        conn.instance_variable_set(:@pgtk_pid, conn.backend_pid)
        conn.instance_variable_set(:@pgtk_closed_at, Time.now)
        conn.instance_variable_set(:@pgtk_closed_reason, reason.gsub(/\s+/, ' ').strip)
        conn.close
      end
    rescue StandardError => e
      @log.warn("Failed to close connection: #{e.message}")
    end
    @wire.connection
  end
end

require_relative 'pool/busy'
require_relative 'pool/iterable_queue'
require_relative 'pool/txn'
