# frozen_string_literal: true

# Copyright (c) 2019-2023 Yegor Bugayenko
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

require 'minitest/autorun'
require 'tmpdir'
require 'pg'
require 'rake'
require 'yaml'
require 'loog'
require_relative '../lib/pgtk/pgsql_task'
require_relative '../lib/pgtk/liquibase_task'
require_relative '../lib/pgtk/pool'

# Pool test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2019 Yegor Bugayenko
# License:: MIT
class TestPool < Minitest::Test
  def test_reads_version
    bootstrap do |pool|
      ver = pool.version
      assert(ver.start_with?('1'))
      assert(!ver.include?(' '))
    end
  end

  def test_basic
    bootstrap do |pool|
      id = pool.exec(
        'INSERT INTO book (title) VALUES ($1) RETURNING id',
        ['Elegant Objects']
      )[0]['id'].to_i
      assert(id.positive?)
    end
  end

  def test_transaction
    bootstrap do |pool|
      id = pool.transaction do |t|
        t.exec('DELETE FROM book')
        t.exec(
          [
            'INSERT INTO book (title)',
            'VALUES ($1) RETURNING id'
          ],
          ['Object Thinking']
        )[0]['id'].to_i
      end
      assert(id.positive?)
    end
  end

  def test_reconnects_on_pg_error
    bootstrap do |pool|
      assert_raises PG::UndefinedTable do
        pool.exec('SELECT * FROM thisiserror')
      end
      5.times do
        pool.exec('SELECT * FROM book')
      end
    end
  end

  def test_reconnects_on_pg_reboot
    port = RandomPort::Pool::SINGLETON.acquire
    Dir.mktmpdir 'test' do |dir|
      id = rand(100..999)
      Pgtk::PgsqlTask.new("pgsql#{id}") do |t|
        t.dir = File.join(dir, 'pgsql')
        t.user = 'hello'
        t.password = 'A B C привет ! & | !'
        t.dbname = 'test'
        t.yaml = File.join(dir, 'cfg.yml')
        t.quiet = true
        t.fresh_start = true
        t.port = port
      end
      task = Rake::Task["pgsql#{id}"]
      task.invoke
      pool = Pgtk::Pool.new(
        Pgtk::Wire::Yaml.new(File.join(dir, 'cfg.yml')),
        log: Loog::VERBOSE
      )
      pool.start(1)
      pool.exec('SELECT * FROM pg_catalog.pg_tables')
      pid = File.read(File.join(dir, 'pgsql/pid')).to_i
      `kill -KILL #{pid}`
      sleep 1
      task.reenable
      task.invoke
      assert_raises(PG::UnableToSend, PG::ConnectionBad) do
        pool.exec('SELECT * FROM pg_catalog.pg_tables')
      end
      pool.exec('SELECT * FROM pg_catalog.pg_tables')
    end
  end

  private

  def bootstrap
    Dir.mktmpdir 'test' do |dir|
      id = rand(100..999)
      Pgtk::PgsqlTask.new("pgsql#{id}") do |t|
        t.dir = File.join(dir, 'pgsql')
        t.user = 'hello'
        t.password = 'A B C привет ! & | !'
        t.dbname = 'test'
        t.yaml = File.join(dir, 'cfg.yml')
        t.quiet = true
      end
      Rake::Task["pgsql#{id}"].invoke
      Pgtk::LiquibaseTask.new("liquibase#{id}") do |t|
        t.master = File.join(__dir__, '../test-resources/master.xml')
        t.yaml = File.join(dir, 'cfg.yml')
        t.quiet = true
      end
      Rake::Task["liquibase#{id}"].invoke
      pool = Pgtk::Pool.new(
        Pgtk::Wire::Yaml.new(File.join(dir, 'cfg.yml')),
        log: Loog::VERBOSE
      )
      pool.start(1)
      yield pool
    end
  end
end
