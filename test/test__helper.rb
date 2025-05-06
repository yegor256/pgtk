# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

$stdout.sync = true

require 'simplecov'
require 'simplecov-cobertura'
unless SimpleCov.running || ENV['PICKS']
  SimpleCov.command_name('test')
  SimpleCov.formatter = SimpleCov::Formatter::MultiFormatter.new(
    [
      SimpleCov::Formatter::HTMLFormatter,
      SimpleCov::Formatter::CoberturaFormatter
    ]
  )
  SimpleCov.minimum_coverage 90
  SimpleCov.minimum_coverage_by_file 70
  SimpleCov.start do
    add_filter 'test/'
    add_filter 'vendor/'
    add_filter 'target/'
    track_files 'lib/**/*.rb'
    track_files '*.rb'
  end
end

require 'minitest/autorun'
require 'minitest/reporters'
Minitest::Reporters.use! [Minitest::Reporters::SpecReporter.new]

require 'logger'
require 'loog'
require 'rake'
require 'rake/tasklib'
require_relative '../lib/pgtk'
require_relative '../lib/pgtk/liquibase_task'
require_relative '../lib/pgtk/pgsql_task'

class Pgtk::Test < Minitest::Test
  def fake_config
    Dir.mktmpdir do |dir|
      id = rand(100_000..999_999)
      f = File.join(dir, 'cfg.yml')
      Pgtk::PgsqlTask.new("pgsql#{id}") do |t|
        t.dir = File.join(dir, 'pgsql')
        t.user = 'hello'
        t.password = 'A B C привет ! & | !'
        t.dbname = 'test'
        t.yaml = f
        t.quiet = true
      end
      Rake::Task["pgsql#{id}"].invoke
      Pgtk::LiquibaseTask.new("liquibase#{id}") do |t|
        t.master = File.join(__dir__, '../test-resources/master.xml')
        t.yaml = f
        t.quiet = true
      end
      Rake::Task["liquibase#{id}"].invoke
      assert_path_exists(f)
      yield f
    end
  end

  def fake_pool(log: Loog::NULL)
    fake_config do |f|
      pool = Pgtk::Pool.new(
        Pgtk::Wire::Yaml.new(f),
        log: log
      )
      pool.start(1)
      yield pool
    end
  end
end
