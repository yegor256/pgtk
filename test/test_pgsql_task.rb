# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'rake'
require 'tmpdir'
require 'yaml'
require_relative 'test__helper'
require_relative '../lib/pgtk/pgsql_task'

# Pgsql rake task test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2025 Yegor Bugayenko
# License:: MIT
class TestPgsqlTask < Pgtk::Test
  def test_basic
    Dir.mktmpdir do |dir|
      Pgtk::PgsqlTask.new(:p2) do |t|
        t.dir = File.join(dir, 'pgsql')
        t.user = 'hello'
        t.password = 'A B C привет ! & | !'
        t.dbname = 'test'
        t.yaml = File.join(dir, 'cfg.yml')
        t.quiet = true
        t.config = {
          log_min_error_statement: 'ERROR'
        }
      end
      Rake::Task['p2'].invoke
      yaml = YAML.load_file(File.join(dir, 'cfg.yml'))
      assert(yaml['pgsql']['url'].start_with?('jdbc:postgresql://localhost'))
    end
  end

  def test_not_quiet
    Dir.mktmpdir do |dir|
      Pgtk::PgsqlTask.new(:p3) do |t|
        t.dir = File.join(dir, 'pgsql')
        t.user = 'hello'
        t.password = 'the password'
        t.dbname = 'test'
        t.yaml = File.join(dir, 'cfg.yml')
        t.quiet = true
        t.config = {
          log_directory: dir,
          logging_collector: 'on',
          log_statement: 'all',
          log_filename: 'pgsql.log'
        }
      end
      Rake::Task['p3'].invoke
      yaml = YAML.load_file(File.join(dir, 'cfg.yml'))
      assert(yaml['pgsql']['url'].start_with?('jdbc:postgresql://localhost'))
      if File.exist?(File.join(dir, 'pid'))
        assert_path_exists(File.join(dir, 'pgsql.log'))
      elsif File.exist?(File.join(dir, 'docker-container'))
        File.read(File.join(dir, 'docker-container')).then do |container|
          assert_equal('ok', qbash("docker exec #{container} bash -c \"test -f #{dir}/pgsql.log && echo 'ok'\""))
        end
      end
    end
  end

  def test_docker
    Dir.mktmpdir do |dir|
      Pgtk::PgsqlTask.new(:pgsql_docker) do |t|
        t.dir = File.join(dir, 'pgsql')
        t.user = 'user'
        t.password = 'pass'
        t.dbname = 'db'
        t.yaml = File.join(dir, 'config.yml')
        t.quiet = true
        t.force_docker = true
        t.config = {
          log_min_error_statement: 'ERROR'
        }
      end
      Rake::Task['pgsql_docker'].invoke
      yaml = YAML.load_file(File.join(dir, 'config.yml'))
      assert(yaml['pgsql']['url'].start_with?('jdbc:postgresql://localhost'))
      container = File.read(File.join(dir, 'pgsql', 'docker-container')).strip
      assert_match('true', qbash("docker inspect --format=\"{{.State.Running}}\" #{container}"))
    end
  end
end
