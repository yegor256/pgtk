# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'loog'
require 'pg'
require 'qbash'
require 'rake'
require 'tmpdir'
require 'yaml'
require_relative '../lib/pgtk/impatient'
require_relative '../lib/pgtk/pool'
require_relative '../lib/pgtk/retry'
require_relative 'test__helper'

# Retry test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2026 Yegor Bugayenko
# License:: MIT
class TestRetry < Pgtk::Test
  def test_takes_version
    fake_pool do |pool|
      v = Pgtk::Retry.new(pool).version
      refute_nil(v)
    end
  end

  def test_dumps_inner_state
    fake_pool do |pool|
      t = Pgtk::Retry.new(pool).dump
      refute_nil(t)
    end
  end

  def test_executes_select_without_error
    fake_pool do |pool|
      r = Pgtk::Retry.new(pool, attempts: 3)
      r.start!
      result = r.exec('SELECT 1 as value')
      assert_equal('1', result.first['value'])
    end
  end

  def test_retries_select_on_failure
    fake_pool do |pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |sql, *args|
        counter += 1
        raise(PG::Error, 'Connection lost') if counter < 3
        pool.exec(sql, *args)
      end
      retrier = Pgtk::Retry.new(stub, attempts: 3)
      result = retrier.exec('SELECT 2 as num')
      assert_equal('2', result.first['num'])
      assert_equal(3, counter)
    end
  end

  def test_fails_after_max_attempts
    fake_pool do |_pool|
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |_sql, *_args|
        raise(PG::Error, 'Persistent failure')
      end
      retrier = Pgtk::Retry.new(stub, attempts: 2)
      e =
        assert_raises(Pgtk::Retry::Exhausted) do
          retrier.exec('SELECT * FROM users')
        end
      assert_kind_of(PG::Error, e.cause, 'original exception must be preserved as cause')
      assert_equal('Persistent failure', e.cause.message, 'original message must be reachable via cause')
    end
  end

  def test_does_not_retry_insert
    fake_pool do |_pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |_sql, *_args|
        counter += 1
        raise(PG::Error, 'Insert failed')
      end
      retrier = Pgtk::Retry.new(stub, attempts: 3)
      assert_raises(PG::Error) do
        retrier.exec('INSERT INTO book (title) VALUES ($1)', ['Test Book'])
      end
      assert_equal(1, counter)
    end
  end

  def test_does_not_retry_update
    fake_pool do |_pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |_sql, *_args|
        counter += 1
        raise(PG::Error, 'Update failed')
      end
      retrier = Pgtk::Retry.new(stub, attempts: 3)
      assert_raises(PG::Error) do
        retrier.exec('UPDATE book SET title = $1 WHERE id = $2', ['New Title', 1])
      end
      assert_equal(1, counter)
    end
  end

  def test_does_not_retry_delete
    fake_pool do |_pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |_sql, *_args|
        counter += 1
        raise(PG::Error, 'Delete failed')
      end
      retrier = Pgtk::Retry.new(stub, attempts: 3)
      assert_raises(PG::Error) do
        retrier.exec('DELETE FROM book WHERE id = $1', [1])
      end
      assert_equal(1, counter)
    end
  end

  def test_handles_select_with_leading_whitespace
    fake_pool do |pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |sql, *args|
        counter += 1
        raise(PG::Error, 'Connection lost') if counter < 2
        pool.exec(sql, *args)
      end
      retrier = Pgtk::Retry.new(stub, attempts: 3)
      result = retrier.exec('  SELECT 3 as value')
      assert_equal('3', result.first['value'])
      assert_equal(2, counter)
    end
  end

  def test_handles_select_case_insensitive
    fake_pool do |pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |sql, *args|
        counter += 1
        raise(PG::Error, 'Connection lost') if counter < 2
        pool.exec(sql, *args)
      end
      retrier = Pgtk::Retry.new(stub, attempts: 3)
      result = retrier.exec('select 4 as value')
      assert_equal('4', result.first['value'])
      assert_equal(2, counter)
    end
  end

  def test_handles_array_sql
    fake_pool do |pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |sql, *args|
        counter += 1
        raise(PG::Error, 'Connection lost') if counter < 2
        pool.exec(sql, *args)
      end
      retrier = Pgtk::Retry.new(stub, attempts: 3)
      result = retrier.exec(%w[SELECT 5 as value])
      assert_equal('5', result.first['value'])
      assert_equal(2, counter)
    end
  end

  def test_transaction_passes_through
    fake_pool do |pool|
      retrier = Pgtk::Retry.new(pool)
      retrier.transaction do |t|
        id = Integer(t.exec('INSERT INTO book (title) VALUES ($1) RETURNING id', ['Transaction Book']).first['id'], 10)
        assert_predicate(id, :positive?)
      end
    end
  end

  def test_preserves_original_error_as_cause
    fake_pool do |_pool|
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |_sql, *_args|
        raise(ArgumentError, 'Invalid argument')
      end
      retrier = Pgtk::Retry.new(stub, attempts: 2)
      e =
        assert_raises(Pgtk::Retry::Exhausted) do
          retrier.exec('SELECT * FROM table')
        end
      assert_kind_of(ArgumentError, e.cause, 'original ArgumentError must be preserved as cause')
    end
  end

  def test_retries_with_unicode_query
    fake_pool do |pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |sql, *args|
        counter += 1
        raise(PG::Error, 'Connection lost') if counter < 2
        pool.exec(sql, *args)
      end
      retrier = Pgtk::Retry.new(stub, attempts: 3)
      result = retrier.exec('SELECT \'привет\' as greeting')
      assert_equal('привет', result.first['greeting'])
      assert_equal(2, counter)
    end
  end

  def test_retries_insert_on_connection_bad
    fake_pool do |pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |sql, *args|
        counter += 1
        raise(PG::ConnectionBad, 'SSL error: decryption failed') if counter < 2
        pool.exec(sql, *args)
      end
      retrier = Pgtk::Retry.new(stub, attempts: 3)
      retrier.exec('INSERT INTO book (title) VALUES ($1)', ['Connection Test'])
      assert_equal(2, counter, 'insert must be retried on PG::ConnectionBad since the query never reached the server')
    end
  end

  def test_retries_update_on_connection_bad
    fake_pool do |pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |sql, *args|
        counter += 1
        raise(PG::ConnectionBad, 'SSL error: bad record mac') if counter < 2
        pool.exec(sql, *args)
      end
      retrier = Pgtk::Retry.new(stub, attempts: 3)
      retrier.exec('UPDATE book SET title = $1 WHERE id = $2', ['Updated', 1])
      assert_equal(2, counter, 'update must be retried on PG::ConnectionBad')
    end
  end

  def test_retries_set_on_connection_bad
    fake_pool do |pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |sql, *args|
        counter += 1
        raise(PG::ConnectionBad, 'PQconsumeInput() SSL error') if counter < 2
        pool.exec(sql, *args)
      end
      retrier = Pgtk::Retry.new(stub, attempts: 3)
      retrier.exec("SET statement_timeout = '15s'")
      assert_equal(2, counter, 'SET must be retried on PG::ConnectionBad')
    end
  end

  def test_retries_select_on_too_slow
    fake_pool do |pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |sql, *args|
        counter += 1
        raise(Pgtk::Impatient::TooSlow, 'query terminated') if counter < 2
        pool.exec(sql, *args)
      end
      retrier = Pgtk::Retry.new(stub, attempts: 3)
      retrier.exec('SELECT 7 as value')
      assert_equal(2, counter, 'select must be retried on Impatient::TooSlow')
    end
  end

  def test_retries_insert_on_too_slow
    fake_pool do |pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |sql, *args|
        counter += 1
        raise(Pgtk::Impatient::TooSlow, 'query terminated') if counter < 3
        pool.exec(sql, *args)
      end
      retrier = Pgtk::Retry.new(stub, attempts: 3)
      retrier.exec('SELECT * FROM book WHERE title = $1', ['Slow Test'])
      assert_equal(3, counter, 'SELECT must be retried on Impatient::TooSlow')
    end
  end

  def test_fails_after_max_too_slow_attempts
    fake_pool do |_pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |_sql, *_args|
        counter += 1
        raise(Pgtk::Impatient::TooSlow, 'persistent slowness')
      end
      retrier = Pgtk::Retry.new(stub, attempts: 2)
      assert_raises(Pgtk::Impatient::TooSlow) do
        retrier.exec('UPDATE book SET title = $1 WHERE id = $2', ['X', 1])
      end
    end
  end

  def test_fails_insert_after_max_connection_bad_attempts
    fake_pool do |_pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |_sql, *_args|
        counter += 1
        raise(PG::ConnectionBad, 'Persistent connection failure')
      end
      retrier = Pgtk::Retry.new(stub, attempts: 3)
      e =
        assert_raises(Pgtk::Retry::Exhausted) do
          retrier.exec('INSERT INTO book (title) VALUES ($1)', ['Fail Test'])
        end
      assert_kind_of(PG::ConnectionBad, e.cause, 'original PG::ConnectionBad must be preserved as cause')
      assert_equal(3, counter, 'insert must be retried up to the configured limit on PG::ConnectionBad')
    end
  end
end
