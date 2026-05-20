# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'cgi'
require 'uri'
require_relative '../../pgtk'
require_relative '../wire'
require_relative 'direct'

# Using ENV variable.
#
# The value of the variable should be in this format:
#
#   postgres://user:password@host:port/dbname
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class Pgtk::Wire::Env
  # Constructor.
  #
  # @param [String] var The name of the environment variable with the connection URL
  # @param [Hash] opts Extra options forwarded to +PG.connect+ (e.g. +sslmode+,
  #   +connect_timeout+, +keepalives+, +keepalives_idle+, +application_name+).
  #   Explicit kwargs win over options carried in the URL query string on conflict.
  def initialize(var = 'DATABASE_URL', **opts)
    raise(ArgumentError, "The name of the environment variable can't be nil") if var.nil?
    @value = ENV.fetch(var, nil)
    raise(ArgumentError, "The environment variable #{@value.inspect} is not set") if @value.nil?
    @opts = opts
  end

  # Create a new connection to PostgreSQL server.
  def connection
    uri = URI(@value)
    Pgtk::Wire::Direct.new(
      host: CGI.unescape(uri.host),
      port: uri.port || 5432,
      dbname: CGI.unescape(uri.path[1..]),
      user: CGI.unescape(uri.userinfo.split(':')[0]),
      password: CGI.unescape(uri.userinfo.split(':')[1]),
      **(uri.query ? URI.decode_www_form(uri.query).to_h.transform_keys(&:to_sym) : {}).merge(@opts)
    ).connection
  end
end
