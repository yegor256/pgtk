# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'pg'
require 'loog'
require_relative '../pgtk'
require_relative 'wire'

# Spy is a decorator for Pool that intercepts and tracks SQL queries.
# It provides observability into database operations by invoking a callback
# with the SQL query and its execution time for each database operation.
#
# This class implements the same interface as Pool, but adds instrumentation
# functionality while delegating actual database operations to the decorated pool.
# Use Spy for debugging, performance monitoring, or audit logging.
#
# Basic usage:
#
#   # Create and configure a regular pool
#   pool = Pgtk::Pool.new(wire)
#   pool.start!(4)
#
#   # Wrap the pool in a spy that tracks all executed queries
#   queries = []
#   spy = Pgtk::Spy.new(pool) do |sql, duration|
#     puts "Query: #{sql}"
#     puts "Duration: #{duration} seconds"
#     queries << sql
#   end
#
#   # Use the spy just like a regular pool, with automatic tracking
#   spy.exec('SELECT * FROM users')
#
#   # Transactions also track each query inside the transaction
#   spy.transaction do |t|
#     t.exec('UPDATE users SET active = true WHERE id = $1', [42])
#     t.exec('INSERT INTO audit_log (user_id, action) VALUES ($1, $2)', [42, 'activated'])
#   end
#
#   # Examine collected queries for analysis
#   puts "Total queries: #{queries.size}"
#   puts "First query: #{queries.first}"
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2025 Yegor Bugayenko
# License:: MIT
class Pgtk::Spy
  # Constructor.
  #
  # @param [Pgtk::Pool] pool The pool to spy on
  # @yield [String, Float] Yields the SQL query and execution time
  def initialize(pool, &block)
    @pool = pool
    @block = block
  end

  # Start a new connection pool with the given arguments.
  def start!(*)
    @pool.start!(*)
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
      'Pgtk::Spy'
    ].join("\n")
  end

  # Execute a SQL query and track its execution.
  #
  # @param [String] sql The SQL query with params inside (possibly)
  # @return [Array] Result rows
  def exec(sql, *)
    start = Time.now
    ret = @pool.exec(sql, *)
    @block&.call(sql.is_a?(Array) ? sql.join(' ') : sql, Time.now - start)
    ret
  end

  # Run a transaction with spying on each SQL query.
  #
  # @yield [Pgtk::Spy] Yields a spy transaction
  # @return [Object] Result of the block
  def transaction
    @pool.transaction do |t|
      yield Pgtk::Spy.new(t, &@block)
    end
  end
end
