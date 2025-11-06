# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require_relative '../pgtk'

# Retry is a decorator for Pool that automatically retries failed SELECT queries.
# It provides fault tolerance for transient database errors by retrying read-only
# operations a configurable number of times before giving up.
#
# This class implements the same interface as Pool but adds retry logic specifically
# for SELECT queries. Non-SELECT queries are executed without retry to maintain
# data integrity and avoid unintended side effects from duplicate writes.
#
# Basic usage:
#
#   # Create and configure a regular pool
#   pool = Pgtk::Pool.new(wire)
#   pool.start!(4)
#
#   # Wrap the pool in a retry decorator with 3 attempts
#   retry_pool = Pgtk::Retry.new(pool, attempts: 3)
#
#   # SELECT queries are automatically retried on failure
#   begin
#     retry_pool.exec('SELECT * FROM users WHERE id = $1', [42])
#   rescue PG::Error => e
#     puts "Query failed after 3 attempts: #{e.message}"
#   end
#
#   # Non-SELECT queries are not retried
#   retry_pool.exec('UPDATE users SET active = true WHERE id = $1', [42])
#
#   # Transactions pass through without retry logic
#   retry_pool.transaction do |t|
#     t.exec('SELECT * FROM accounts')  # No retry within transaction
#     t.exec('UPDATE accounts SET balance = balance + 100')
#   end
#
#   # Combining with other decorators
#   impatient = Pgtk::Impatient.new(retry_pool, 5)
#   spy = Pgtk::Spy.new(impatient) do |sql, duration|
#     puts "Query: #{sql} (#{duration}s)"
#   end
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2025 Yegor Bugayenko
# License:: MIT
class Pgtk::Retry
  # Constructor.
  #
  # @param [Pgtk::Pool] pool The pool to decorate
  # @param [Integer] attempts Number of attempts to make (default: 3)
  def initialize(pool, attempts: 3)
    @pool = pool
    @attempts = attempts
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
      "Pgtk::Retry (attempts=#{@attempts})"
    ].join("\n")
  end

  # Execute a SQL query with automatic retry for SELECT queries.
  #
  # @param [String] sql The SQL query with params inside (possibly)
  # @return [Array] Result rows
  def exec(sql, *)
    query = sql.is_a?(Array) ? sql.join(' ') : sql
    if query.strip.upcase.start_with?('SELECT')
      attempt = 0
      begin
        @pool.exec(sql, *)
      rescue StandardError => e
        attempt += 1
        raise e if attempt >= @attempts
        retry
      end
    else
      @pool.exec(sql, *)
    end
  end

  # Run a transaction without retry logic.
  #
  # @yield [Object] Yields the transaction object
  # @return [Object] Result of the block
  def transaction(&)
    @pool.transaction(&)
  end
end
