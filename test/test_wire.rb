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

  def test_omits_credentials_when_url_has_no_userinfo
    ENV['DATABASE_URL_NO_USER'] = 'postgres://localhost:5432/testdb'
    args = captured { Pgtk::Wire::Env.new('DATABASE_URL_NO_USER').connection }
    assert_nil(args[:user], 'a URL without userinfo must pass no user to libpq')
    assert_nil(args[:password], 'a URL without userinfo must pass no password to libpq')
    assert_equal('testdb', args[:dbname], args.to_s)
  end

  def test_takes_a_user_without_a_password
    ENV['DATABASE_URL_NO_PASSWORD'] = 'postgres://jeff@localhost:5432/testdb'
    args = captured { Pgtk::Wire::Env.new('DATABASE_URL_NO_PASSWORD').connection }
    assert_equal('jeff', args[:user], args.to_s)
    assert_nil(args[:password], 'a URL without a password must pass no password to libpq')
  end

  def test_complains_when_the_database_name_is_absent
    ENV['DATABASE_URL_NO_DBNAME'] = 'postgres://localhost:5432'
    assert_includes(
      assert_raises(ArgumentError) { Pgtk::Wire::Env.new('DATABASE_URL_NO_DBNAME').connection }.message,
      'database name is absent',
      'a URL without a database name must be reported, not crash in CGI.unescape'
    )
  end

  private

  def captured(&)
    args = {}
    Pgtk::Wire.stub_const(
      :Direct,
      Class.new do
        define_method(:initialize) do |**opts|
          args.replace(opts)
        end
        define_method(:connection) { args }
      end,
      &
    )
    args
  end
end
