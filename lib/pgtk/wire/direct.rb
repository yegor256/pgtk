# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'pg'
require_relative '../../pgtk'

module Pgtk::Wire; end

# Simple wire with details.
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class Pgtk::Wire::Direct
  # Constructor.
  #
  # @param [String] host Host name of the PostgreSQL server
  # @param [Integer] port Port number of the PostgreSQL server
  # @param [String] dbname Database name
  # @param [String] user Username
  # @param [String] password Password
  # @param [Hash] opts Extra options forwarded to +PG.connect+ (e.g. +sslmode+,
  #   +connect_timeout+, +keepalives+, +keepalives_idle+, +application_name+)
  def initialize(host:, port:, dbname:, user:, password:, **opts)
    raise(ArgumentError, "The host can't be nil") if host.nil?
    @host = host
    raise(ArgumentError, "The port can't be nil") if port.nil?
    @port = port
    @dbname = dbname
    @user = user
    @password = password
    @opts = opts
  end

  # Create a new connection to PostgreSQL server.
  def connection
    PG.connect(dbname: @dbname, host: @host, port: @port, user: @user, password: @password, **@opts)
  end
end
