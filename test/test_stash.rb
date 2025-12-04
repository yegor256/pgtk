# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'threads'
require_relative 'test__helper'
require_relative '../lib/pgtk/pool'
require_relative '../lib/pgtk/stash'

# Pool test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2025 Yegor Bugayenko
# License:: MIT
class TestStash < Pgtk::Test
  def test_simple_insert
    fake_pool do |pool|
      id = Pgtk::Stash.new(pool).exec(
        'INSERT INTO book (title) VALUES ($1) RETURNING id',
        ['Elegant Objects']
      )[0]['id'].to_i
      assert_predicate(id, :positive?)
    end
  end

  def test_non_trivial_queries
    fake_pool do |pool|
      pg = Pgtk::Stash.new(pool)
      [
        'VACUUM FULL',
        'START TRANSACTION',
        'REINDEX TABLE book',
        'TRUNCATE book',
        'CREATE TABLE tmp (id INT)',
        'ALTER TABLE tmp ADD COLUMN foo INT',
        'DROP TABLE tmp',
        'SET client_min_messages TO WARNING',
        "SET TIME ZONE 'America/Los_Angeles'"
      ].each do |q|
        pg.exec(q)
      end
    end
  end

  def test_caching
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      query = 'SELECT count(*) FROM book'
      first_result = stash.exec(query)
      second_result = stash.exec(query)
      assert_equal(first_result.to_a, second_result.to_a)
      assert_same(first_result, second_result)
    end
  end

  def test_cache_invalidation
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      query = 'SELECT count(*) FROM book'
      first = stash.exec(query)
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['New Book'])
      second = stash.exec(query)
      refute_same(first, second)
    end
  end

  def test_caching_with_params
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      query = 'SELECT * FROM book WHERE title = $1'
      first_result = stash.exec(query, ['Elegant Objects'])
      second_result = stash.exec(query, ['Elegant Objects'])
      assert_equal(first_result.to_a, second_result.to_a)
      assert_same(first_result, second_result)
      different_param_result = stash.exec(query, ['Different Title'])
      refute_same(first_result, different_param_result)
    end
  end

  def test_query_with_semicolon
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      assert_empty(stash.exec('SELECT * FROM book ;'))
      assert_empty(stash.exec('SELECT * FROM book;'))
    end
  end

  def test_raise_no_tables_error
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      assert_raises(RuntimeError) { stash.exec('SELECT 1;') }
      assert_raises(RuntimeError) { stash.exec('SELECT * FROM generate_series(1, 5);') }
    end
  end

  def test_version
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      assert_match(/^\d+\.\d+/, stash.version)
    end
  end

  def test_transaction
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['Transaction Test'])
      stash.transaction do |tx|
        result = tx.exec('SELECT title FROM book WHERE title = $1', ['Transaction Test'])
        assert_equal('Transaction Test', result[0]['title'])
        true
      end
    end
  end

  def test_start
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['Start Test'])
      stash.start!
      result = stash.exec('SELECT title FROM book WHERE title = $1', ['Start Test'])
      assert_equal('Start Test', result[0]['title'])
    end
  end

  def test_dump_empty_inner_state
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.start!
      stash.dump.then do |d|
        assert_includes(d, '0 stale queries in cache')
        assert_includes(d, '0 tables in cache')
      end
    end
  end

  def test_dump_not_started_inner_state
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.dump.then do |d|
        assert_includes(d, 'Not launched')
        assert_includes(d, '0 stale queries in cache')
        assert_includes(d, '0 tables in cache')
      end
    end
  end

  def test_dump_inner_state
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['My book'])
      7.times do
        stash.exec('SELECT id, title FROM book WHERE title = $1 ORDER BY id DESC', ['My book'])
      end
      8.times do
        stash.exec('SELECT id, title FROM book WHERE title = $1 ORDER BY id DESC', ['My book 2'])
      end
      5.times do
        stash.exec('SELECT title FROM book WHERE id = $1', [1])
      end
      3.times do
        stash.exec('SELECT title FROM book WHERE id = $1', [2])
      end
      stash.dump.then do |d|
        assert_includes(d, '0 stale queries in cache')
        assert_includes(d, '2 other queries in cache')
        assert_includes(d, '1 tables in cache')
        assert_includes(d, '2/15p/0s/')
        assert_includes(d, ': SELECT id, title')
        assert_includes(d, '2/8p/0s/')
        assert_includes(d, ': SELECT title')
      end
    end
  end

  def test_cache_refills
    refill_interval = 0.2
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, refill_interval:)
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['My book'])
      stash.exec('SELECT id, title FROM book WHERE title = $1 ORDER BY id DESC', ['My book'])
      stash.exec('SELECT id, title FROM book WHERE title = $1 ORDER BY id DESC', ['My book'])
      stash.exec('SELECT id, title FROM book WHERE title = $1 ORDER BY id DESC', ['My book'])
      assert_includes(stash.dump, '1/3p/0s/')
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['My book'])
      assert_includes(stash.dump, '1/3p/1s/')
      sleep refill_interval * 2
      assert_includes(stash.dump, '1/3p/0s/')
    end
  end

  def test_cache_refill_respects_pause
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, refill_interval: 0.1, refill_delay: 4)
      stash.start!
      stash.exec('SELECT * FROM book')
      assert_includes(stash.dump, '1/1p/0s/')
      stash.exec("INSERT INTO book (title) VALUES ('Elegant Objects')")
      assert_includes(stash.dump, '1/1p/1s/')
      sleep 0.2
      assert_includes(stash.dump, '1/1p/1s/')
    end
  end

  def test_caps_oldest_queries
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, cap: 1, cap_interval: 0.1)
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['1984'])
      5.times do |i|
        stash.exec('SELECT * FROM book WHERE id = $1', [i])
        sleep 0.01
      end
      stash.exec('SELECT id FROM book')
      sleep 0.01
      stash.exec('SELECT title FROM book')
      sleep 0.2
      assert_includes(stash.dump, '1/1p/0s')
    end
  end

  def test_retire_oldest_queries
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, retire: 0.1, cap_interval: 0.1)
      stash.start!
      stash.exec('SELECT * FROM book WHERE id = $1', [1])
      assert_includes(stash.dump, '1/1p/0s')
      sleep 0.3
      assert_includes(stash.dump, '1/1p/0s')
    end
  end

  def test_not_count_queries_that_uncached
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['My book'])
      stash.exec('SELECT id, title, NOW() FROM book WHERE id = $1', [1])
      refute_includes(stash.dump, 'SELECT id, title, NOW()')
    end
  end

  def test_dumps_with_spaces_in_query
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['Test'])
      stash.exec('SELECT  title  FROM    book  WHERE id = $1', [1])
      result = stash.dump
      assert_includes(result, 'queries in cache')
    end
  end

  # To reproduce fail test, you need add `sleep 2` to `@stash` iterator block.
  # For example:
  #   @stash[:queries]
  #        .map { |k, v| sleep 2; [k, v.values.sum { |vv| vv[:popularity] }, v.values.any? { |vv| vv[:stale] }] }
  #   ...
  #   @stash[:queries][q].each_keys do |k|
  #      sleep 2
  #      # ...
  #   end
  def test_capture_entrance_in_stash_iterators_with_multithreading
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, threads: 1, refill_interval: 1)
      stash.start!
      Threads.new(10).assert(10) do
        sleep 0.25
        10.times do
          stash.exec('INSERT INTO book (title) VALUES ($1)', ['My book'])
          stash.exec('SELECT id, title FROM book WHERE id = $1', [rand(1..100)])
          stash.exec("SELECT id, title FROM book WHERE id = $1 OR 1 = #{rand(1..100)}", [rand(1..100)])
          sleep 0.25
        end
        sleep 0.25
      end
    end
  end
end
