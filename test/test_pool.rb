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

require 'loog'
require 'minitest/autorun'
require 'pg'
require 'qbash'
require 'rake'
require 'tmpdir'
require 'yaml'
require_relative '../lib/pgtk/liquibase_task'
require_relative '../lib/pgtk/pgsql_task'
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

  def test_complex_query
    bootstrap do |pool|
      pool.exec(
        "
        INSERT INTO book (title) VALUES ('one');
        INSERT INTO book (title) VALUES ('two');
        "
      )
    end
  end

  def test_logs_sql
    log = Loog::Buffer.new
    bootstrap(log: log) do |pool|
      pool.exec(
        'INSERT INTO book (title) VALUES ($1)',
        ['Object Thinking']
      )
      assert(log.to_s.include?('INSERT INTO book (title) VALUES ($1)'))
    end
  end

  def test_logs_errors
    log = Loog::Buffer.new
    bootstrap(log: log) do |pool|
      assert_raises PG::UndefinedTable do
        pool.exec('INSERT INTO tableDoesNotExist (a) VALUES (42)')
      end
      assert(log.to_s.include?('INSERT INTO tableDoesNotExist'))
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

  def test_transaction_with_error
    bootstrap do |pool|
      pool.exec('DELETE FROM book')
      assert(pool.exec('SELECT * FROM book').empty?)
      assert_raises do
        pool.transaction do |t|
          t.exec('INSERT INTO book (title) VALUES ($1)', ['hey'])
          t.exec('INSERT INTO book (error_here) VALUES ($1)', ['hey'])
        end
      end
      assert(pool.exec('SELECT * FROM book').empty?)
      pool.exec('INSERT INTO book (title) VALUES ($1)', ['another'])
      assert(!pool.exec('SELECT * FROM book').empty?)
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
        log: Loog::NULL
      )
      pool.start(1)
      pool.exec('SELECT * FROM pg_catalog.pg_tables')
      qbash("pg_ctl -D #{Shellwords.escape(File.join(dir, 'pgsql'))} stop", log: $stdout)
      cycle = 0
      loop do
        begin
          TCPSocket.new('localhost', port)
          sleep(0.1)
          cycle += 1
          if cycle > 50
            qbash('ps -ax | grep postgres')
            raise "Can't stop running postgres at port #{port}, for some reason"
          end
        rescue StandardError => e
          puts e.message
          break
        end
      end
      assert_raises(PG::UnableToSend, PG::ConnectionBad) do
        pool.exec('SELECT * FROM pg_catalog.pg_tables')
      end
      task.reenable
      task.invoke
      loop do
        begin
          pool.exec('SELECT * FROM pg_catalog.pg_tables')
          break
        rescue StandardError => e
          puts e.message
          sleep(0.1)
          retry
        end
      end
    end
  end

  private

  def bootstrap(log: Loog::NULL)
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
        log: log
      )
      pool.start(1)
      yield pool
    end
  end
end
