# frozen_string_literal: true

# Copyright (c) 2019-2024 Yegor Bugayenko
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the 'Software'), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

require 'pg'
require 'uri'
require 'yaml'
require_relative '../pgtk'

# Wires.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2024 Yegor Bugayenko
# License:: MIT
module Pgtk::Wire
end

# Simple wire with details.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2024 Yegor Bugayenko
# License:: MIT
class Pgtk::Wire::Direct
  # Constructor.
  def initialize(host:, port:, dbname:, user:, password:)
    raise "The host can't be nil" if host.nil?
    @host = host
    raise "The host can't be nil" if host.nil?
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
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2024 Yegor Bugayenko
# License:: MIT
class Pgtk::Wire::Env
  # Constructor.
  def initialize(var = 'DATABASE_URL')
    raise "The name of the environmant variable can't be nil" if var.nil?
    @var = var
  end

  # Create a new connection to PostgreSQL server.
  def connection
    v = ENV.fetch(@var, nil)
    raise "The environment variable #{@var.inspect} is not set" if v.nil?
    uri = URI(v)
    Pgtk::Wire::Direct.new(
      host: uri.host,
      port: uri.port,
      dbname: uri.path[1..-1],
      user: uri.userinfo.split(':')[0],
      password: uri.userinfo.split(':')[1]
    ).connection
  end
end

# Using configuration from YAML file.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2024 Yegor Bugayenko
# License:: MIT
class Pgtk::Wire::Yaml
  # Constructor.
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
    Pgtk::Wire::Direct.new(
      host: cfg['pgsql']['host'],
      port: cfg['pgsql']['port'],
      dbname: cfg['pgsql']['dbname'],
      user: cfg['pgsql']['user'],
      password: cfg['pgsql']['password']
    ).connection
  end
end
