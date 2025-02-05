# frozen_string_literal: true

# Copyright (c) 2019-2025 Yegor Bugayenko
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
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

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
    ex = assert_raises do
      Rake::Task['lb'].invoke
    end
    assert(ex.message.include?('the-file-doesnt-exist.xml'))
  end
end
