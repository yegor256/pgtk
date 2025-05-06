# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'timeout'
require_relative '../pgtk'

# An impatient decorator of a pool — it doesn't wait too long on every
# request, but terminates them and fails.
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2025 Yegor Bugayenko
# License:: MIT
class Pgtk::Impatient
  # Constructor.
  #
  # @param [Pgtk::Pool] pool The pool to decorate
  # @param [Integer] timeout Timeout in seconds for each SQL query
  def initialize(pool, timeout = 1)
    @pool = pool
    @timeout = timeout
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
    Timeout.timeout(@timeout) do
      @pool.exec(sql, *args)
    end
  end

  # Run a transaction with a timeout for each query.
  #
  # @yield [Pgtk::Impatient] Yields an impatient transaction
  # @return [Object] Result of the block
  def transaction
    @pool.transaction do |t|
      yield Pgtk::Impatient.new(t, @timeout)
    end
  end
end
