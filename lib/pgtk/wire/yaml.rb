# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'yaml'
require_relative '../../pgtk'
require_relative 'direct'

module Pgtk::Wire; end

# Using configuration from YAML file.
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class Pgtk::Wire::Yaml
  # Constructor.
  #
  # @param [String] file Path to the YAML configuration file
  # @param [String] node The root node name in the YAML file containing PostgreSQL configuration
  def initialize(file, node = 'pgsql')
    raise(ArgumentError, "The name of the file can't be nil") if file.nil?
    @file = file
    raise(ArgumentError, "The name of the node in the YAML file can't be nil") if node.nil?
    @node = node
  end

  # Create a new connection to PostgreSQL server.
  def connection
    raise(ArgumentError, "The file #{@file.inspect} not found") unless File.exist?(@file)
    cfg = ::YAML.load_file(@file)
    raise(ArgumentError, "The node '#{@node}' not found in YAML file #{@file.inspect}") unless cfg[@node]
    Pgtk::Wire::Direct.new(
      host: cfg[@node]['host'],
      port: cfg[@node]['port'],
      dbname: cfg[@node]['dbname'],
      user: cfg[@node]['user'],
      password: cfg[@node]['password']
    ).connection
  end
end
