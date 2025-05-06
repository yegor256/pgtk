# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'pg'
require 'loog'
require_relative '../pgtk'
require_relative 'wire'

# A pool that spies on another pool.
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

  # Get the version of PostgreSQL server.
  #
  # @return [String] Version of PostgreSQL server
  def version
    @pool.version
  end

  # Execute a SQL query and track its execution.
  #
  # @param [String] sql The SQL query with params inside (possibly)
  # @param [Array] args List of arguments
  # @return [Array] Result rows
  def exec(sql, *args)
    start = Time.now
    ret = @pool.exec(sql, *args)
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
