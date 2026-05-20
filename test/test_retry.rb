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
      refute_nil(Pgtk::Retry.new(pool).version)
    end
  end

  def test_dumps_inner_state
    fake_pool do |pool|
      refute_nil(Pgtk::Retry.new(pool).dump)
    end
  end

  def test_executes_select_without_error
    fake_pool do |pool|
      r = Pgtk::Retry.new(pool, attempts: 3)
      r.start!
      assert_equal('1', r.exec('SELECT 1 as value').first['value'])
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
      assert_equal('2', Pgtk::Retry.new(stub, attempts: 3).exec('SELECT 2 as num').first['num'])
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
      assert_equal('3', Pgtk::Retry.new(stub, attempts: 3).exec('  SELECT 3 as value').first['value'])
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
      assert_equal('4', Pgtk::Retry.new(stub, attempts: 3).exec('select 4 as value').first['value'])
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
      assert_equal('5', Pgtk::Retry.new(stub, attempts: 3).exec(%w[SELECT 5 as value]).first['value'])
      assert_equal(2, counter)
    end
  end

  def test_transaction_passes_through
    fake_pool do |pool|
      retrier = Pgtk::Retry.new(pool)
      retrier.transaction do |t|
        assert_predicate(
          Integer(
            t.exec('INSERT INTO book (title) VALUES ($1) RETURNING id', ['Transaction Book']).first['id'],
            10
          ), :positive?
        )
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
      assert_kind_of(
        ArgumentError,
        assert_raises(Pgtk::Retry::Exhausted) do
          retrier.exec('SELECT * FROM table')
        end.cause,
        'original ArgumentError must be preserved as cause'
      )
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
      assert_equal('привет', Pgtk::Retry.new(stub, attempts: 3).exec('SELECT \'привет\' as greeting').first['greeting'])
      assert_equal(2, counter)
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
      Pgtk::Retry.new(stub, attempts: 3).exec('SELECT 7 as value')
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
      Pgtk::Retry.new(stub, attempts: 3).exec('SELECT * FROM book WHERE title = $1', ['Slow Test'])
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

  def test_does_not_retry_non_select_on_connection_bad
    fake_pool do |_pool|
      counter = 0
      stub = Object.new
      def stub.version
        'stub'
      end
      stub.define_singleton_method(:exec) do |_sql, *_args|
        counter += 1
        raise(PG::ConnectionBad, 'server closed the connection unexpectedly')
      end
      retrier = Pgtk::Retry.new(stub, attempts: 3)
      assert_raises(PG::ConnectionBad) do
        retrier.exec('INSERT INTO book (title) VALUES ($1)', ['No Retry'])
      end
      assert_equal(
        1, counter,
        'non-SELECT must not be retried on PG::ConnectionBad: the response may be lost after the server got the query'
      )
    end
  end
end
