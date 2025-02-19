# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'minitest/autorun'
require 'tmpdir'
require 'rake'
require 'yaml'
require_relative '../lib/pgtk/pgsql_task'
require_relative '../lib/pgtk/liquibase_task'

# Liquibase rake task test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2025 Yegor Bugayenko
# License:: MIT
class TestLiquibaseTask < Minitest::Test
  def test_basic
    Dir.mktmpdir 'test' do |dir|
      Pgtk::PgsqlTask.new(:pgsql2) do |t|
        t.dir = File.join(dir, 'pgsql')
        t.user = 'hello'
        t.password = 'A B C привет ! & | !'
        t.dbname = 'test'
        t.yaml = File.join(dir, 'cfg.yml')
        t.quiet = true
      end
      Rake::Task['pgsql2'].invoke
      Pgtk::LiquibaseTask.new(:liquibase2) do |t|
        t.master = File.join(__dir__, '../test-resources/master.xml')
        t.yaml = ['file-is-absent', File.join(dir, 'cfg.yml'), 'another']
        t.quiet = true
        t.postgresql_version = '42.7.0'
        t.liquibase_version = '3.2.2'
      end
      Rake::Task['liquibase2'].invoke
    end
  end

  def test_latest_version
    Dir.mktmpdir 'test' do |dir|
      Pgtk::PgsqlTask.new(:pgsql) do |t|
        t.dir = File.join(dir, 'pgsql')
        t.user = 'xxx'
        t.password = 'xxx'
        t.dbname = 'xxx'
        t.yaml = File.join(dir, 'xxx.yml')
        t.quiet = true
      end
      Rake::Task['pgsql'].invoke
      Pgtk::LiquibaseTask.new(:liquibase) do |t|
        t.master = File.join(__dir__, '../test-resources/master.xml')
        t.yaml = File.join(dir, 'xxx.yml')
        t.postgresql_version = '42.7.1'
        t.liquibase_version = '4.25.1'
        t.quiet = true
      end
      Rake::Task['liquibase'].invoke
    end
  end

  def test_with_invalid_master_file
    Pgtk::LiquibaseTask.new(:lb) do |t|
      t.master = 'the-file-doesnt-exist.xml'
      t.yaml = { 'pgsql' => {} }
      t.quiet = true
    end
    ex = assert_raises(StandardError) do
      Rake::Task['lb'].invoke
    end
    assert_includes(ex.message, 'the-file-doesnt-exist.xml')
  end
end
