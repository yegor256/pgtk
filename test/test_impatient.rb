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
require_relative '../lib/pgtk/impatient'

# Pool test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2025 Yegor Bugayenko
# License:: MIT
class TestImpatient < Pgtk::Test
  def test_takes_version
    fake_pool do |pool|
      v = Pgtk::Impatient.new(pool, 1).version
      refute_nil(v)
    end
  end

  def test_dumps_inner_state
    fake_pool do |pool|
      t = Pgtk::Impatient.new(pool, 1).dump
      refute_nil(t)
    end
  end

  def test_interrupts
    fake_pool do |pool|
      assert_raises(Pgtk::Impatient::TooSlow) do
        Pgtk::Impatient.new(pool, 0.01).exec(
          [
            'SELECT COUNT(*)',
            'FROM generate_series(1, 1000000) AS a'
          ]
        )
      end
    end
  end

  def test_skips_by_regex
    fake_pool do |pool|
      Pgtk::Impatient.new(pool, 0.01, /^SELECT.*$/).exec(
        [
          'SELECT COUNT(*)',
          'FROM generate_series(1, 1000000) AS a'
        ]
      )
    end
  end

  def test_doesnt_shadow_larger_timeout
    fake_pool do |pool|
      assert_raises(Timeout::Error) do
        Timeout.timeout(0.1) do
          Pgtk::Impatient.new(pool, 999).exec(
            'SELECT COUNT(*) FROM generate_series(1, 100000000) AS a'
          )
        end
      end
    end
  end

  def test_doesnt_interrupt
    fake_pool do |pool|
      id = Pgtk::Impatient.new(pool, 1).exec(
        'INSERT INTO book (title) VALUES ($1) RETURNING id',
        ['1984']
      ).first['id'].to_i
      assert_predicate(id, :positive?)
    end
  end

  def test_doesnt_interrupt_in_transaction
    fake_pool do |pool|
      Pgtk::Impatient.new(pool, 1).transaction do |t|
        id = t.exec(
          'INSERT INTO book (title) VALUES ($1) RETURNING id',
          ['1984']
        ).first['id'].to_i
        assert_predicate(id, :positive?)
      end
    end
  end
end
