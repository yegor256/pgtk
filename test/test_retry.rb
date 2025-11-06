# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'loog'
require 'pg'
require 'qbash'
require 'rake'
require 'tmpdir'
require 'yaml'
require_relative 'test__helper'
require_relative '../lib/pgtk/pool'
require_relative '../lib/pgtk/retry'

# Retry test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2025 Yegor Bugayenko
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
      r.start!(1)
      result = r.exec('SELECT 1 as value')
      assert_equal('1', result.first['value'])
    end
  end

  def test_retries_select_on_failure
    fake_pool do |pool|
      counter = 0
      stub_pool = Object.new
      def stub_pool.version
        'stub'
      end
      stub_pool.define_singleton_method(:exec) do |sql, *args|
        counter += 1
        raise PG::Error, 'Connection lost' if counter < 3
        pool.exec(sql, *args)
      end
      retry_pool = Pgtk::Retry.new(stub_pool, attempts: 3)
      result = retry_pool.exec('SELECT 2 as num')
      assert_equal('2', result.first['num'])
      assert_equal(3, counter)
    end
  end

  def test_fails_after_max_attempts
    fake_pool do |_pool|
      stub_pool = Object.new
      def stub_pool.version
        'stub'
      end
      stub_pool.define_singleton_method(:exec) do |_sql, *_args|
        raise PG::Error, 'Persistent failure'
      end
      retry_pool = Pgtk::Retry.new(stub_pool, attempts: 2)
      assert_raises(PG::Error) do
        retry_pool.exec('SELECT * FROM users')
      end
    end
  end

  def test_does_not_retry_insert
    fake_pool do |_pool|
      counter = 0
      stub_pool = Object.new
      def stub_pool.version
        'stub'
      end
      stub_pool.define_singleton_method(:exec) do |_sql, *_args|
        counter += 1
        raise PG::Error, 'Insert failed'
      end
      retry_pool = Pgtk::Retry.new(stub_pool, attempts: 3)
      assert_raises(PG::Error) do
        retry_pool.exec('INSERT INTO book (title) VALUES ($1)', ['Test Book'])
      end
      assert_equal(1, counter)
    end
  end

  def test_does_not_retry_update
    fake_pool do |_pool|
      counter = 0
      stub_pool = Object.new
      def stub_pool.version
        'stub'
      end
      stub_pool.define_singleton_method(:exec) do |_sql, *_args|
        counter += 1
        raise PG::Error, 'Update failed'
      end
      retry_pool = Pgtk::Retry.new(stub_pool, attempts: 3)
      assert_raises(PG::Error) do
        retry_pool.exec('UPDATE book SET title = $1 WHERE id = $2', ['New Title', 1])
      end
      assert_equal(1, counter)
    end
  end

  def test_does_not_retry_delete
    fake_pool do |_pool|
      counter = 0
      stub_pool = Object.new
      def stub_pool.version
        'stub'
      end
      stub_pool.define_singleton_method(:exec) do |_sql, *_args|
        counter += 1
        raise PG::Error, 'Delete failed'
      end
      retry_pool = Pgtk::Retry.new(stub_pool, attempts: 3)
      assert_raises(PG::Error) do
        retry_pool.exec('DELETE FROM book WHERE id = $1', [1])
      end
      assert_equal(1, counter)
    end
  end

  def test_handles_select_with_leading_whitespace
    fake_pool do |pool|
      counter = 0
      stub_pool = Object.new
      def stub_pool.version
        'stub'
      end
      stub_pool.define_singleton_method(:exec) do |sql, *args|
        counter += 1
        raise PG::Error, 'Connection lost' if counter < 2
        pool.exec(sql, *args)
      end
      retry_pool = Pgtk::Retry.new(stub_pool, attempts: 3)
      result = retry_pool.exec('  SELECT 3 as value')
      assert_equal('3', result.first['value'])
      assert_equal(2, counter)
    end
  end

  def test_handles_select_case_insensitive
    fake_pool do |pool|
      counter = 0
      stub_pool = Object.new
      def stub_pool.version
        'stub'
      end
      stub_pool.define_singleton_method(:exec) do |sql, *args|
        counter += 1
        raise PG::Error, 'Connection lost' if counter < 2
        pool.exec(sql, *args)
      end
      retry_pool = Pgtk::Retry.new(stub_pool, attempts: 3)
      result = retry_pool.exec('select 4 as value')
      assert_equal('4', result.first['value'])
      assert_equal(2, counter)
    end
  end

  def test_handles_array_sql
    fake_pool do |pool|
      counter = 0
      stub_pool = Object.new
      def stub_pool.version
        'stub'
      end
      stub_pool.define_singleton_method(:exec) do |sql, *args|
        counter += 1
        raise PG::Error, 'Connection lost' if counter < 2
        pool.exec(sql, *args)
      end
      retry_pool = Pgtk::Retry.new(stub_pool, attempts: 3)
      result = retry_pool.exec(%w[SELECT 5 as value])
      assert_equal('5', result.first['value'])
      assert_equal(2, counter)
    end
  end

  def test_transaction_passes_through
    fake_pool do |pool|
      retry_pool = Pgtk::Retry.new(pool)
      retry_pool.transaction do |t|
        id = t.exec(
          'INSERT INTO book (title) VALUES ($1) RETURNING id',
          ['Transaction Book']
        ).first['id'].to_i
        assert_predicate(id, :positive?)
      end
    end
  end

  def test_preserves_original_error_type
    fake_pool do |_pool|
      stub_pool = Object.new
      def stub_pool.version
        'stub'
      end
      stub_pool.define_singleton_method(:exec) do |_sql, *_args|
        raise ArgumentError, 'Invalid argument'
      end
      retry_pool = Pgtk::Retry.new(stub_pool, attempts: 2)
      assert_raises(ArgumentError) do
        retry_pool.exec('SELECT * FROM table')
      end
    end
  end

  def test_retries_with_unicode_query
    fake_pool do |pool|
      counter = 0
      stub_pool = Object.new
      def stub_pool.version
        'stub'
      end
      stub_pool.define_singleton_method(:exec) do |sql, *args|
        counter += 1
        raise PG::Error, 'Connection lost' if counter < 2
        pool.exec(sql, *args)
      end
      retry_pool = Pgtk::Retry.new(stub_pool, attempts: 3)
      result = retry_pool.exec('SELECT \'привет\' as greeting')
      assert_equal('привет', result.first['greeting'])
      assert_equal(2, counter)
    end
  end
end
