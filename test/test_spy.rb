# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require_relative 'test__helper'
require_relative '../lib/pgtk/pool'
require_relative '../lib/pgtk/spy'

# Pool test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2026 Yegor Bugayenko
# License:: MIT
class TestSpy < Pgtk::Test
  def test_simple_insert
    fake_pool do |pool|
      id = Pgtk::Spy.new(pool).exec(
        'INSERT INTO book (title) VALUES ($1) RETURNING id',
        ['Elegant Objects']
      )[0]['id'].to_i
      assert_predicate(id, :positive?)
    end
  end

  def test_version
    fake_pool do |pool|
      stash = Pgtk::Spy.new(pool)
      assert_match(/^\d+\.\d+/, stash.version)
    end
  end

  def test_transaction
    fake_pool do |pool|
      stash = Pgtk::Spy.new(pool)
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
      stash = Pgtk::Spy.new(pool)
      stash.exec(
        [
          'INSERT INTO book (title)',
          'VALUES ($1)'
        ],
        ['Start Test']
      )
      stash.start!
      result = stash.exec('SELECT title FROM book WHERE title = $1', ['Start Test'])
      assert_equal('Start Test', result[0]['title'])
    end
  end
end
