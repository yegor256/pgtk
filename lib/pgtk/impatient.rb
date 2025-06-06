# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'securerandom'
require 'tago'
require 'timeout'
require_relative '../pgtk'

# Impatient is a decorator for Pool that enforces timeouts on all database operations.
# It ensures that SQL queries don't run indefinitely, which helps prevent application
# hangs and resource exhaustion when database operations are slow or stalled.
#
# This class implements the same interface as Pool but wraps each database operation
# in a timeout block. If a query exceeds the specified timeout, it raises a Timeout::Error
# exception, allowing the application to handle slow queries gracefully.
#
# Basic usage:
#
#   # Create and configure a regular pool
#   pool = Pgtk::Pool.new(wire).start(4)
#
#   # Wrap the pool in an impatient decorator with a 2-second timeout
#   impatient = Pgtk::Impatient.new(pool, 2)
#
#   # Execute queries with automatic timeout enforcement
#   begin
#     impatient.exec('SELECT * FROM large_table WHERE complex_condition')
#   rescue Timeout::Error
#     puts "Query timed out after 2 seconds"
#   end
#
#   # Transactions also enforce timeouts on each query
#   begin
#     impatient.transaction do |t|
#       t.exec('UPDATE large_table SET processed = true')
#       t.exec('DELETE FROM queue WHERE processed = true')
#     end
#   rescue Timeout::Error
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
# Copyright:: Copyright (c) 2019-2025 Yegor Bugayenko
# License:: MIT
class Pgtk::Impatient
  # If timed out
  class TooSlow < StandardError; end

  # Constructor.
  #
  # @param [Pgtk::Pool] pool The pool to decorate
  # @param [Integer] timeout Timeout in seconds for each SQL query
  # @param [Array<Regex>] off List of regex to exclude queries from checking
  def initialize(pool, timeout, *off)
    @pool = pool
    @timeout = timeout
    @off = off
  end

  # Get the version of PostgreSQL server.
  #
  # @return [String] Version of PostgreSQL server
  def version
    @pool.version
  end

  # Execute a SQL query with a timeout.
  #
  # @param [String] sql The SQL query with params inside (possibly)
  # @param [Array] args List of arguments
  # @return [Array] Result rows
  # @raise [Timeout::Error] If the query takes too long
  def exec(sql, *args)
    return @pool.exec(sql, *args) if @off.any? { |re| re.match?(sql) }
    start = Time.now
    token = SecureRandom.uuid
    begin
      Timeout.timeout(@timeout, Timeout::Error, token) do
        @pool.exec(sql, *args)
      end
    rescue Timeout::Error => e
      raise e unless e.message == token
      raise TooSlow, [
        'SQL query',
        ("with #{args.count} argument#{'s' if args.count > 1}" unless args.empty?),
        'was terminated after',
        start.ago,
        'of waiting'
      ].compact.join(' ')
    end
  end

  # Run a transaction with a timeout for each query.
  #
  # @yield [Pgtk::Impatient] Yields an impatient transaction
  # @return [Object] Result of the block
  def transaction
    @pool.transaction do |t|
      t.exec("SET LOCAL statement_timeout = #{(@timeout * 1000).to_i}")
      yield Pgtk::Impatient.new(t, @timeout)
    end
  end
end
