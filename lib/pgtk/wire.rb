# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'pg'
require 'uri'
require 'yaml'
require_relative '../pgtk'

# Wires.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2025 Yegor Bugayenko
# License:: MIT
module Pgtk::Wire
end

# Simple wire with details.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2025 Yegor Bugayenko
# License:: MIT
class Pgtk::Wire::Direct
  # Constructor.
  #
  # @param [String] host Host name of the PostgreSQL server
  # @param [Integer] port Port number of the PostgreSQL server
  # @param [String] dbname Database name
  # @param [String] user Username
  # @param [String] password Password
  def initialize(host:, port:, dbname:, user:, password:)
    raise "The host can't be nil" if host.nil?
    @host = host
    raise "The port can't be nil" if port.nil?
    @port = port
    @dbname = dbname
    @user = user
    @password = password
  end

  # Create a new connection to PostgreSQL server.
  def connection
    PG.connect(
      dbname: @dbname, host: @host, port: @port,
      user: @user, password: @password
    )
  end
end

# Using ENV variable.
#
# The value of the variable should be in this format:
#
#   postgres://user:password@host:port/dbname
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2025 Yegor Bugayenko
# License:: MIT
class Pgtk::Wire::Env
  # Constructor.
  #
  # @param [String] var The name of the environment variable with the connection URL
  def initialize(var = 'DATABASE_URL')
    raise "The name of the environment variable can't be nil" if var.nil?
    @var = var
  end

  # Create a new connection to PostgreSQL server.
  def connection
    v = ENV.fetch(@var, nil)
    raise "The environment variable #{@var.inspect} is not set" if v.nil?
    uri = URI(v)
    Pgtk::Wire::Direct.new(
      host: CGI.unescape(uri.host),
      port: uri.port,
      dbname: CGI.unescape(uri.path[1..]),
      user: CGI.unescape(uri.userinfo.split(':')[0]),
      password: CGI.unescape(uri.userinfo.split(':')[1])
    ).connection
  end
end

# Using configuration from YAML file.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2025 Yegor Bugayenko
# License:: MIT
class Pgtk::Wire::Yaml
  # Constructor.
  #
  # @param [String] file Path to the YAML configuration file
  # @param [String] node The root node name in the YAML file containing PostgreSQL configuration
  def initialize(file, node = 'pgsql')
    raise "The name of the file can't be nil" if file.nil?
    @file = file
    raise "The name of the node in the YAML file can't be nil" if node.nil?
    @node = node
  end

  # Create a new connection to PostgreSQL server.
  def connection
    raise "The file #{@file.inspect} not found" unless File.exist?(@file)
    cfg = YAML.load_file(@file)
    raise "The node '#{@node}' not found in YAML file #{@file.inspect}" unless cfg[@node]
    Pgtk::Wire::Direct.new(
      host: cfg[@node]['host'],
      port: cfg[@node]['port'],
      dbname: cfg[@node]['dbname'],
      user: cfg[@node]['user'],
      password: cfg[@node]['password']
    ).connection
  end
end
