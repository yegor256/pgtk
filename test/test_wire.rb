# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'cgi'
require 'yaml'
require_relative 'test__helper'
require_relative '../lib/pgtk/wire'

# Wire test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2026 Yegor Bugayenko
# License:: MIT
class TestWire < Pgtk::Test
  def test_connects
    fake_config do |f|
      wire = Pgtk::Wire::Yaml.new(f)
      c = wire.connection
      refute_nil(c)
    end
  end

  def test_connects_via_env_variable
    fake_config do |f|
      c = YAML.load_file(f)['pgsql']
      v = 'DATABASE_URL'
      ENV[v] = [
        "postgres://#{CGI.escape(c['user'])}:#{CGI.escape(c['password'])}",
        "@#{CGI.escape(c['host'])}:#{CGI.escape(c['port'].to_s)}/#{CGI.escape(c['dbname'])}"
      ].join
      wire = Pgtk::Wire::Env.new(v)
      c = wire.connection
      refute_nil(c)
    end
  end
end
