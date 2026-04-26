# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'loog'
require 'pg'
require 'qbash'
require 'rake'
require 'securerandom'
require 'threads'
require 'tmpdir'
require 'yaml'
require_relative '../lib/pgtk/liquibase_task'
require_relative '../lib/pgtk/pgsql_task'
require_relative '../lib/pgtk/pool'
require_relative '../lib/pgtk/spy'
require_relative 'test__helper'

# Pool test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2026 Yegor Bugayenko
# License:: MIT
class TestPool < Pgtk::Test
  def test_reads_version
    fake_pool do |pool|
      ver = pool.version
      assert(ver.start_with?('1'))
      refute_includes(ver, ' ')
    end
  end

  def test_dumps_itself
    fake_pool do |pool|
      t = pool.dump
      assert_includes(t, pool.version)
    end
  end

  def test_dumps_connections
    fake_pool(4) do |pool|
      t = pool.dump
      assert_includes(t, '4 connections')
      assert_includes(t, pool.version)
    end
  end

  def test_dumps_different_statuses
    fake_pool(4) do |pool|
      Threads.new.assert do
        10.times do
          pool.exec('INSERT INTO book (title) VALUES ($1)', [SecureRandom.hex(30)])
          pool.dump
          pool.exec('SELECT * FROM book')
        end
      end
    end
  end

  def test_basic
    fake_pool do |pool|
      id = Integer(pool.exec('INSERT INTO book (title) VALUES ($1) RETURNING id', ['Elegant Objects'])[0]['id'], 10)
      assert_predicate(id, :positive?)
    end
  end

  def test_logs_pgsql_errors_to_logger
    buf = Loog::Buffer.new
    fake_pool(log: buf) do |pool|
      pool.exec(
        "
        CREATE FUNCTION intentional_failure() RETURNS trigger AS
        'BEGIN
          IF NEW.title = ''War and War'' THEN
            RAISE EXCEPTION ''The title of the book is bad'';
          END IF;
          RETURN NEW;
        END' LANGUAGE PLPGSQL
        "
      )
      pool.exec(
        "
        CREATE TRIGGER check_book_title
        BEFORE INSERT ON book
        FOR EACH ROW EXECUTE PROCEDURE intentional_failure();
        "
      )
      assert_raises(PG::RaiseException) do
        pool.exec('INSERT INTO book (title) VALUES ($1)', ['War and War'])
      end
      assert_includes(buf.to_s, 'The title of the book is bad')
      assert_includes(buf.to_s, 'function intentional_failure() line 3')
    end
  end

  def test_queries_with_block
    fake_pool do |pool|
      pool.exec('INSERT INTO book (title) VALUES ($1)', ['1984'])
      rows = []
      pool.exec('SELECT * FROM book') do |row|
        rows.append(row)
      end
      assert_equal(1, rows.size)
    end
  end

  def test_with_spy
    queries = []
    fake_pool do |pool|
      pool = Pgtk::Spy.new(pool) { |sql| queries.append(sql) }
      pool.exec(['INSERT INTO book', '(title) VALUES ($1)'], ['Elegant Objects'])
    end
    assert_equal(1, queries.size)
    assert_equal('INSERT INTO book (title) VALUES ($1)', queries.first)
  end

  def test_complex_query
    fake_pool do |pool|
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
    fake_pool(log: log) do |pool|
      pool.exec('INSERT INTO book (title) VALUES ($1)', ['Object Thinking'])
      assert_includes(log.to_s, 'INSERT INTO book (title) VALUES ($1)')
    end
  end

  def test_logs_errors
    log = Loog::Buffer.new
    fake_pool(log: log) do |pool|
      assert_raises(PG::UndefinedTable) do
        pool.exec('INSERT INTO tableDoesNotExist (a) VALUES (42)')
      end
      assert_includes(log.to_s, 'INSERT INTO tableDoesNotExist')
    end
  end

  def test_transaction
    fake_pool do |pool|
      id =
        Pgtk::Spy.new(pool).transaction do |t|
          t.exec('DELETE FROM book')
          Integer(t.exec(['INSERT INTO book (title)', 'VALUES ($1) RETURNING id'], ['Object Thinking'])[0]['id'], 10)
        end
      assert_predicate(id, :positive?)
    end
  end

  def test_transaction_with_error
    fake_pool do |pool|
      pool.exec('DELETE FROM book')
      assert_empty(pool.exec('SELECT * FROM book'))
      assert_raises(StandardError) do
        pool.transaction do |t|
          t.exec('INSERT INTO book (title) VALUES ($1)', ['hey'])
          t.exec('INSERT INTO book (error_here) VALUES ($1)', ['hey'])
        end
      end
      assert_empty(pool.exec('SELECT * FROM book'))
      pool.exec('INSERT INTO book (title) VALUES ($1)', ['another'])
      refute_empty(pool.exec('SELECT * FROM book'))
    end
  end

  def test_reconnects_on_pg_error
    fake_pool do |pool|
      assert_raises(PG::UndefinedTable) do
        pool.exec('SELECT * FROM thisiserror')
      end
      5.times do
        pool.exec('SELECT * FROM book')
      end
    end
  end

  def test_raises_busy_when_no_connection_appears_in_time
    fake_config do |f|
      pool = Pgtk::Pool.new(Pgtk::Wire::Yaml.new(f), max: 1, timeout: 0.1, log: Loog::NULL)
      pool.start!
      holder = Thread.new { pool.exec('SELECT pg_sleep(1)') }
      sleep(0.2)
      assert_raises(Pgtk::Pool::Busy) { pool.exec('SELECT 1') }
      holder.join
    end
  end

  def test_renews_connections_left_in_failed_transaction
    fake_pool(2) do |pool|
      pool.exec('SELECT 1')
      queue = pool.instance_variable_get(:@pool)
      queue.map do |c|
        c.exec('BEGIN')
        begin
          c.exec('SELECT * FROM nonexistent_table_zzz')
        rescue PG::UndefinedTable
          nil
        end
      end
      Threads.new(2).assert(15) do
        pool.transaction do |t|
          assert_equal('42', t.exec('SELECT 42 AS n')[0]['n'])
        end
      end
    end
  end

  def test_rolls_back_transaction_on_non_standard_error
    fake_pool do |pool|
      pool.exec('DELETE FROM book')
      assert_raises(SystemExit) do
        pool.transaction do |t|
          t.exec('INSERT INTO book (title) VALUES ($1)', ['x'])
          raise(SystemExit)
        end
      end
      assert_empty(pool.exec('SELECT * FROM book'), 'transaction must rollback even on non-StandardError')
    end
  end

  def test_dumps_closed_connection_with_timestamp
    fake_pool(2) do |pool|
      pool.version
      items = pool.instance_variable_get(:@pool).instance_variable_get(:@items)
      pool.__send__(:renew, items.first, 'some reason')
      assert_match(/connection is closed,\s+\S+ ago/, pool.dump, 'dump must include time since connection was closed')
    end
  end

  def test_dumps_closed_connection_with_reason
    fake_pool(2) do |pool|
      pool.version
      items = pool.instance_variable_get(:@pool).instance_variable_get(:@items)
      pool.__send__(:renew, items.first, 'forced shutdown by test')
      assert_match(/because: forced shutdown by test/, pool.dump, 'dump must include reason for closing')
    end
  end

  def test_dumps_closed_connection_with_last_query
    fake_pool(2) do |pool|
      pool.version
      pool.exec('SELECT 42 AS marker_query')
      items = pool.instance_variable_get(:@pool).instance_variable_get(:@items)
      hit = items.find { |c| c.instance_variable_get(:@pgtk_last_query)&.include?('marker_query') }
      refute_nil(hit, 'no connection has executed the marker query')
      pool.__send__(:renew, hit, 'closed by test')
      assert_match(/last query: SELECT 42 AS marker_query/, pool.dump, 'dump must include last executed query')
    end
  end

  def test_dumps_running_query_for_active_connection
    fake_pool(2) do |pool|
      pool.version
      items = pool.instance_variable_get(:@pool).instance_variable_get(:@items)
      conn = items.first
      conn.instance_variable_set(:@pgtk_last_query, 'SELECT pg_sleep(1)')
      conn.send_query('SELECT pg_sleep(1)')
      begin
        assert_match(/running: SELECT pg_sleep/, pool.dump, 'dump must include currently running query')
      ensure
        while (r = conn.get_result)
          r.clear
        end
      end
    end
  end

  def test_ellipsizes_long_running_query_in_dump
    fake_pool(2) do |pool|
      pool.version
      items = pool.instance_variable_get(:@pool).instance_variable_get(:@items)
      conn = items.first
      long = "SELECT pg_sleep(1) /* #{'x' * 200} */"
      conn.instance_variable_set(:@pgtk_last_query, long)
      conn.send_query(long)
      begin
        assert_match(/running: .{1,60}\.\.\./, pool.dump, 'long running query must be ellipsized to 60 chars')
      ensure
        while (r = conn.get_result)
          r.clear
        end
      end
    end
  end

  def test_renews_dead_connections_proactively
    fake_pool(3) do |pool|
      pool.exec('SELECT 1')
      queue = pool.instance_variable_get(:@pool)
      queue.map { |c| c.close unless c.finished? }
      Threads.new(3).assert(15) do
        assert_equal('42', pool.exec('SELECT 42 AS n')[0]['n'])
      end
      refute_includes(pool.dump, 'connection is closed', 'dead connections must not linger in the pool')
    end
  end

  def test_reconnects_on_pg_reboot
    port = RandomPort::Pool::SINGLETON.acquire
    Dir.mktmpdir do |dir|
      id = rand(100..999)
      task = fake_pgsql(dir, id, port)
      pool = Pgtk::Pool.new(Pgtk::Wire::Yaml.new(File.join(dir, 'cfg.yml')), max: 1, log: Loog::NULL)
      spin(50) { pool.start! }
      pool.exec('SELECT * FROM pg_catalog.pg_tables')
      halt(dir)
      drain(dir, port)
      assert_raises(PG::UnableToSend, PG::ConnectionBad) do
        pool.exec('SELECT * FROM pg_catalog.pg_tables')
      end
      task.reenable
      task.invoke
      loop do
        pool.exec('SELECT * FROM pg_catalog.pg_tables')
        break
      rescue StandardError => e
        puts(e.message)
        sleep(0.1)
        retry
      end
    end
  end

  private

  def fake_pgsql(dir, id, port)
    Pgtk::PgsqlTask.new("pgsql#{id}") do |t|
      t.dir = File.join(dir, 'pgsql')
      t.user = 'hello'
      t.password = 'A B C привет ! & | !'
      t.dbname = 'test'
      t.yaml = File.join(dir, 'cfg.yml')
      t.quiet = true
      t.fresh = true
      t.port = port
    end
    task = Rake::Task["pgsql#{id}"]
    task.invoke
    task
  end

  def spin(limit)
    cycle = 0
    loop do
      yield
      break
    rescue PG::ConnectionBad
      cycle += 1
      sleep(0.1)
      raise(IOError, "Can't connect after #{limit} attempts") if cycle > limit
    end
  end

  def halt(dir)
    if File.exist?(File.join(dir, 'pgsql', 'pid'))
      qbash("pg_ctl -D #{Shellwords.escape(File.join(dir, 'pgsql'))} stop")
    elsif File.exist?(File.join(dir, 'pgsql', 'docker-container'))
      qbash("docker stop #{File.read(File.join(dir, 'pgsql', 'docker-container'))}")
    end
  end

  def drain(dir, port)
    cycle = 0
    loop do
      TCPSocket.new('localhost', port)
      sleep(0.1)
      cycle += 1
      if cycle > 50
        if File.exist?(File.join(dir, 'pid'))
          qbash('ps -ax | grep postgres')
        elsif File.exist?(File.join(dir, 'docker-container'))
          qbash('docker ps -a | grep postgres')
        end
        raise(IOError, "Can't stop running postgres at port #{port}, for some reason")
      end
    rescue StandardError => e
      puts(e.message)
      break
    end
  end
end
