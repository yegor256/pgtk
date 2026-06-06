# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'cgi'
require 'securerandom'
require 'yaml'
require_relative '../lib/pgtk/wire'
require_relative 'test__helper'

# Wire test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2026 Yegor Bugayenko
# License:: MIT
class TestWire < Pgtk::Test
  def test_connects
    fake_config do |f|
      refute_nil(Pgtk::Wire::Yaml.new(f).connection)
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
      c = Pgtk::Wire::Env.new(v).connection
      refute_nil(c)
    end
  end

  def test_defaults_port_when_missing
    fake_config do |f|
      c = YAML.load_file(f)['pgsql']
      v = 'DATABASE_URL_NO_PORT'
      ENV[v] = "postgres://#{CGI.escape(c['user'])}:#{CGI.escape(c['password'])}@localhost/#{CGI.escape(c['dbname'])}"
      wire = Pgtk::Wire::Env.new(v)
      assert_includes(
        assert_raises(PG::ConnectionBad, 'must attempt connection to default port') do
          wire.connection
        end.message, 'port 5432', 'must default to port 5432 when port is omitted from URL'
      )
    end
  end

  def test_honors_url_query_options_in_env
    fake_config do |f|
      c = YAML.load_file(f)['pgsql']
      v = 'DATABASE_URL_QUERY'
      name = "pgtk_#{SecureRandom.hex(4)}"
      ENV[v] = [
        "postgres://#{CGI.escape(c['user'])}:#{CGI.escape(c['password'])}",
        "@#{CGI.escape(c['host'])}:#{c['port']}/#{CGI.escape(c['dbname'])}",
        "?application_name=#{name}"
      ].join
      assert_equal(
        name,
        Pgtk::Wire::Env.new(v).connection.exec("SELECT current_setting('application_name')")[0]['current_setting'],
        'URL query options must be passed through to PG.connect'
      )
    end
  end

  def test_forwards_extra_opts_via_direct
    fake_config do |f|
      c = YAML.load_file(f)['pgsql']
      name = "pgtk_#{SecureRandom.hex(4)}"
      assert_equal(
        name,
        Pgtk::Wire::Direct.new(
          host: c['host'], port: c['port'], dbname: c['dbname'],
          user: c['user'], password: c['password'],
          application_name: name
        ).connection.exec("SELECT current_setting('application_name')")[0]['current_setting'],
        'extra kwargs on Direct must reach PG.connect'
      )
    end
  end

  def test_yaml_forwards_extra_opts
    fake_config do |f|
      c = YAML.load_file(f)
      c['pgsql']['application_name'] = "pgtk_#{SecureRandom.hex(4)}"
      File.write(f, YAML.dump(c))
      assert_equal(
        c['pgsql']['application_name'],
        Pgtk::Wire::Yaml.new(f).connection.exec(
          "SELECT current_setting('application_name')"
        )[0]['current_setting'],
        'extra YAML keys must reach PG.connect'
      )
    end
  end

  def test_explicit_kwargs_win_over_url_query
    fake_config do |f|
      c = YAML.load_file(f)['pgsql']
      v = 'DATABASE_URL_PRECEDENCE'
      ENV[v] = [
        "postgres://#{CGI.escape(c['user'])}:#{CGI.escape(c['password'])}",
        "@#{CGI.escape(c['host'])}:#{c['port']}/#{CGI.escape(c['dbname'])}",
        '?application_name=from_url'
      ].join
      explicit = "pgtk_#{SecureRandom.hex(4)}"
      assert_equal(
        explicit,
        Pgtk::Wire::Env.new(v, application_name: explicit).connection.exec(
          "SELECT current_setting('application_name')"
        )[0]['current_setting'],
        'explicit kwargs must override URL query options on conflict'
      )
    end
  end
end
