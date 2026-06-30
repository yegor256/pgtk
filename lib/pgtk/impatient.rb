# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'ellipsized'
require 'pg'
require 'tago'
require_relative '../pgtk'

# Impatient is a decorator for Pool that enforces timeouts on all database operations.
# It ensures that SQL queries don't run indefinitely, which helps prevent application
# hangs and resource exhaustion when database operations are slow or stalled.
#
# This class implements the same interface as Pool but enforces the timeout on the
# server side, by wrapping each query in a tiny transaction that issues
# +SET LOCAL statement_timeout+. PostgreSQL itself terminates the query at the
# deadline, which guarantees that the server-side connection slot is freed even
# when the client cannot deliver a cancellation request (for example, behind a
# transaction-pool PgBouncer that does not forward client disconnects to in-flight
# server queries). On timeout, +TooSlow+ is raised.
#
# Queries that match one of the +off+ regular expressions are excluded from
# this checking. They are never wrapped in a transaction, because some
# statements (such as +VACUUM+, +REINDEX+, or +CREATE INDEX CONCURRENTLY+)
# cannot run inside a transaction block. Instead, a session-level
# +SET statement_timeout+ is applied on the same connection before the query
# runs (and reset afterwards), using the +default+ fallback timeout. Pass
# +default: 0+ to run excluded queries with no timeout at all.
#
# Basic usage:
#
#   # Create and configure a regular pool
#   pool = Pgtk::Pool.new(wire, max: 4)
#   pool.start!
#
#   # Wrap the pool in an impatient decorator with a 2-second timeout
#   impatient = Pgtk::Impatient.new(pool, 2)
#
#   # Execute queries with automatic timeout enforcement
#   begin
#     impatient.exec('SELECT * FROM large_table WHERE complex_condition')
#   rescue Pgtk::Impatient::TooSlow
#     puts "Query timed out after 2 seconds"
#   end
#
#   # Transactions also enforce timeouts on each query
#   begin
#     impatient.transaction do |t|
#       t.exec('UPDATE large_table SET processed = true')
#       t.exec('DELETE FROM queue WHERE processed = true')
#     end
#   rescue PG::QueryCanceled
#     puts "Transaction timed out"
#   end
#
#   # Combining with Spy for timeout monitoring
#   spy = Pgtk::Spy.new(impatient) do |sql, duration|
#     puts "Query completed in #{duration} seconds: #{sql}"
#   end
#
#   # Now queries are both timed and monitored
#   spy.exec('SELECT * FROM users')
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class Pgtk::Impatient
  # Constructor.
  #
  # @param [Pgtk::Pool] pool The pool to decorate
  # @param [Integer] timeout Timeout in seconds for each SQL query
  # @param [Array<Regex>] off List of regex to exclude queries from checking
  # @param [Integer] default Fallback timeout in seconds for excluded queries (0 = no timeout)
  def initialize(pool, timeout, *off, default: 300)
    @pool = pool
    @timeout = timeout
    @off = off
    @default = default
  end

  # Start a new connection pool with the given arguments.
  def start!
    @pool.start!
  end

  # Get the version of PostgreSQL server.
  #
  # @return [String] Version of PostgreSQL server
  def version
    @pool.version
  end

  # Convert internal state into text.
  def dump
    [
      @pool.dump,
      '',
      "Pgtk::Impatient (timeout=#{@timeout}s, default=#{@default}s):",
      @off.map { |re| "  #{re}" }
    ].join("\n")
  end

  # Execute a SQL query with a server-side timeout.
  #
  # The query is wrapped in a tiny transaction that issues
  # +SET LOCAL statement_timeout+, so PostgreSQL itself terminates the query
  # at the deadline. This guarantees the server-side connection slot is freed
  # even when the client cannot deliver a cancellation request (for example,
  # behind a transaction-pool PgBouncer). When the deadline fires, the
  # underlying +PG::QueryCanceled+ is translated to +TooSlow+.
  #
  # Queries matching one of the +off+ regular expressions bypass this
  # transaction. They run on a single connection without a transaction block,
  # guarded by a session-level +SET statement_timeout+ (the +default+ fallback,
  # reset afterwards) or by no timeout at all when +default+ is zero. This keeps
  # statements that cannot run inside a transaction, such as +VACUUM+ or
  # +REINDEX+, working as expected.
  #
  # @param [String, Array] query The SQL query with params inside (possibly)
  # @param [Array] args List of arguments
  # @return [Array] Result rows
  # @raise [TooSlow] If the query takes too long
  def exec(query, *args)
    sql = query.is_a?(Array) ? query.join(' ') : query
    if @off.any? { |re| re.match?(sql) }
      ms = Integer(@default * 1000)
      return @pool.exec(sql, *args) if ms.zero?
      return @pool.session do |t|
        t.exec("SET statement_timeout = #{ms}")
        t.exec(sql, *args).tap { t.exec('RESET statement_timeout') }
      end
    end
    start = Time.now
    ms = [Integer(@timeout * 1000), 1].max
    begin
      @pool.transaction do |t|
        t.exec("SET LOCAL statement_timeout = #{ms}")
        t.exec(sql, *args)
      end
    rescue PG::QueryCanceled
      raise(
        TooSlow, [
          'SQL query',
          ("with #{args.count} argument#{'s' if args.count > 1}" unless args.empty?),
          'was terminated after',
          start.ago,
          'of waiting:',
          sql.ellipsized(50).inspect
        ].compact.join(' ')
      )
    end
  end

  # Run a transaction with a timeout for each query and for idle time inside
  # the transaction. If the transaction stays in the +INTRANS+ state (idle
  # inside transaction) for longer than the configured timeout, PostgreSQL
  # terminates the session, which frees locks and releases the connection
  # slot back to the pool.
  #
  # @yield [Object] Yields a transaction object that responds to +exec+
  # @return [Object] Result of the block
  def transaction
    @pool.transaction do |t|
      ms = [Integer(@timeout * 1000), 1].max
      t.exec("SET LOCAL statement_timeout = #{ms}")
      t.exec("SET LOCAL idle_in_transaction_session_timeout = #{ms}")
      yield(t)
    end
  end
end

require_relative 'impatient/too_slow'
