# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'pg'
require 'loog'
require 'tago'
require_relative '../pgtk'
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
#   pool = Pgtk::Pool.new(wire).start(4)
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
# Copyright:: Copyright (c) 2019-2025 Yegor Bugayenko
# License:: MIT
class Pgtk::Pool
  # Constructor.
  #
  # @param [Pgtk::Wire] wire The wire
  # @param [Object] log The log
  def initialize(wire, log: Loog::NULL)
    @wire = wire
    @log = log
    @pool = IterableQueue.new
  end

  # Get the version of PostgreSQL server.
  #
  # @return [String] Version of PostgreSQL server
  def version
    @version ||= exec('SHOW server_version')[0]['server_version'].split[0]
  end

  # Get as much details about it as possible.
  #
  # @return [String] Summary of inner state
  def dump
    [
      "PgSQL version: #{version}",
      "#{@pool.size} connections:",
      @pool.map do |c|
        "  #{c.inspect}"
      end
    ].flatten.join("\n")
  end

  # Start it with a fixed number of connections. The amount of connections
  # is specified in +max+ argument and should be big enough to handle
  # the amount of parallel connections you may have to the database. However,
  # keep in mind that not all servers will allow you to have many connections
  # open at the same time. For example, Heroku free PostgreSQL database
  # allows only one connection open.
  #
  # @param [Integer] max Total amount of PostgreSQL connections in the pool
  def start(max = 8)
    max.times do
      @pool << @wire.connection
    end
    @log.debug("PostgreSQL pool started with #{max} connections")
    self
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
  # All values in the retrieved hash are strings. No matter what types of
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
        r = yield t
        t.exec('COMMIT')
        r
      rescue StandardError => e
        t.exec('ROLLBACK')
        raise e
      end
    end
  end

  # Thread-safe queue implementation that supports iteration.
  # Unlike Ruby's Queue class, this implementation allows safe iteration
  # over all elements while maintaining thread safety for concurrent access.
  #
  # This class is used internally by Pool to store database connections
  # and provide the ability to iterate over them for inspection purposes.
  class IterableQueue
    def initialize
      @items = []
      @mutex = Mutex.new
      @condition = ConditionVariable.new
    end

    def <<(item)
      @mutex.synchronize do
        @items << item
        @condition.signal
      end
    end

    def pop
      @mutex.synchronize do
        @condition.wait(@mutex) while @items.empty?
        @items.shift
      end
    end

    def size
      @mutex.synchronize do
        @items.size
      end
    end

    def map(&)
      @mutex.synchronize do
        @items.map(&)
      end
    end
  end

  # A temporary class to execute a single SQL request.
  class Txn
    def initialize(conn, log)
      @conn = conn
      @log = log
    end

    # Exec a single parameterized command.
    # @param [String] query The SQL query with params inside (possibly)
    # @param [Array] args List of arguments
    # @param [Integer] result Should be 0 for text results, 1 for binary
    # @yield [Hash] Rows
    def exec(query, args = [], result = 0)
      start = Time.now
      sql = query.is_a?(Array) ? query.join(' ') : query
      begin
        out =
          if args.empty?
            @conn.exec(sql) do |res|
              if block_given?
                yield res
              else
                res.each.to_a
              end
            end
          else
            @conn.exec_params(sql, args, result) do |res|
              if block_given?
                yield res
              else
                res.each.to_a
              end
            end
          end
      rescue StandardError => e
        @log.error("#{sql}: #{e.message}")
        raise e
      end
      lag = Time.now - start
      if lag < 1
        @log.debug("#{sql}: #{start.ago} / #{@conn.object_id}")
      else
        @log.info("#{sql}: #{start.ago}")
      end
      out
    end
  end

  private

  def connect
    conn = @pool.pop
    begin
      yield conn
    rescue StandardError => e
      conn = renew(conn)
      raise e
    ensure
      @pool << conn
    end
  end

  def renew(conn)
    begin
      conn.close unless conn.finished?
    rescue StandardError => e
      @log.warn("Failed to close connection: #{e.message}")
    end
    @wire.connection
  end
end
