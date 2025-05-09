# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

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

  def test_caching
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      query = 'SELECT count(*) FROM book'
      first_result = stash.exec(query)
      second_result = stash.exec(query)
      assert_equal(first_result.to_a, second_result.to_a)
      assert_equal(first_result.object_id, second_result.object_id)
    end
  end

  def test_cache_invalidation
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      query = 'SELECT count(*) FROM book'
      first_result = stash.exec(query)
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['New Book'])
      second_result = stash.exec(query)
      refute_equal(first_result.object_id, second_result.object_id)
    end
  end

  def test_caching_with_params
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      query = 'SELECT * FROM book WHERE title = $1'
      first_result = stash.exec(query, ['Elegant Objects'])
      second_result = stash.exec(query, ['Elegant Objects'])
      assert_equal(first_result.to_a, second_result.to_a)
      assert_equal(first_result.object_id, second_result.object_id)
      different_param_result = stash.exec(query, ['Different Title'])
      refute_equal(first_result.object_id, different_param_result.object_id)
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
      new_stash = stash.start(1)
      assert_instance_of(Pgtk::Stash, new_stash)
      result = new_stash.exec('SELECT title FROM book WHERE title = $1', ['Start Test'])
      assert_equal('Start Test', result[0]['title'])
    end
  end
end
