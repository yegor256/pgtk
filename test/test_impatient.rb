# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'loog'
require 'pg'
require 'qbash'
require 'rake'
require 'securerandom'
require 'tmpdir'
require 'yaml'
require_relative '../lib/pgtk/impatient'
require_relative '../lib/pgtk/pool'
require_relative '../lib/pgtk/spy'
require_relative 'test__helper'

# Pool test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2026 Yegor Bugayenko
# License:: MIT
class TestImpatient < Pgtk::Test
  def test_takes_version
    fake_pool do |pool|
      refute_nil(Pgtk::Impatient.new(pool, 1).version)
    end
  end

  def test_dumps_inner_state
    fake_pool do |pool|
      refute_nil(Pgtk::Impatient.new(pool, 1).dump)
    end
  end

  def test_interrupts
    fake_pool do |pool|
      assert_raises(Pgtk::Impatient::TooSlow) do
        Pgtk::Impatient.new(pool, 0.01).exec(['SELECT COUNT(*)', 'FROM generate_series(1, 1000000) AS a'])
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
          Pgtk::Impatient.new(pool, 999).exec('SELECT COUNT(*) FROM generate_series(1, 100000000) AS a')
        end
      end
    end
  end

  def test_doesnt_interrupt
    fake_pool do |pool|
      assert_predicate(
        Integer(
          Pgtk::Impatient.new(pool, 1).exec(
            'INSERT INTO book (title) VALUES ($1) RETURNING id',
            ['1984']
          ).first['id'], 10
        ), :positive?
      )
    end
  end

  def test_doesnt_interrupt_in_transaction
    fake_pool do |pool|
      Pgtk::Impatient.new(pool, 1).transaction do |t|
        assert_predicate(
          Integer(t.exec('INSERT INTO book (title) VALUES ($1) RETURNING id', ['1984']).first['id'], 10), :positive?
        )
      end
    end
  end

  def test_terminates_idle_in_transaction
    fake_pool do |pool|
      assert_raises(StandardError) do
        Pgtk::Impatient.new(pool, 0.2).transaction do |t|
          t.exec('SELECT 1')
          sleep(2)
          t.exec('SELECT 1')
        end
      end
    end
  end

  def test_sets_server_side_timeout_per_query
    fake_pool do |pool|
      captured = []
      Pgtk::Impatient.new(Pgtk::Spy.new(pool) { |sql, _| captured << sql }, 1.5).exec('SELECT 1')
      assert(
        captured.any? { |s| s.match?(/SET LOCAL statement_timeout\s*=\s*\d+/) },
        "must set statement_timeout per query, got: #{captured.inspect}"
      )
    end
  end

  def test_excluded_still_timed_out
    fake_pool do |pool|
      captured = []
      Pgtk::Impatient.new(Pgtk::Spy.new(pool) { |sql, _| captured << sql }, 1.5, /^SELECT 1/).exec('SELECT 1')
      assert(
        captured.any? { |s| s.match?(/SET statement_timeout\s*=\s*300000\b/) },
        "must set default 300s statement_timeout for excluded queries, got: #{captured.inspect}"
      )
      refute(
        captured.any? { |s| s.include?('SET LOCAL') || s.include?('START TRANSACTION') },
        "excluded queries must not run inside a transaction, got: #{captured.inspect}"
      )
    end
  end

  def test_excluded_custom_default
    fake_pool do |pool|
      captured = []
      Pgtk::Impatient.new(
        Pgtk::Spy.new(pool) do |sql, _|
          captured << sql
        end, 1.5, /^SELECT 1/, default: 10
      ).exec('SELECT 1')
      assert(
        captured.any? { |s| s.match?(/SET statement_timeout\s*=\s*10000\b/) },
        "must set custom default timeout, got: #{captured.inspect}"
      )
    end
  end

  def test_excluded_zero_default
    fake_pool do |pool|
      captured = []
      Pgtk::Impatient.new(
        Pgtk::Spy.new(pool) do |sql, _|
          captured << sql
        end, 1.5, /^SELECT 1/, default: 0
      ).exec('SELECT 1')
      refute(
        captured.any? { |s| s.include?('statement_timeout') },
        "must not set statement_timeout when default is 0, got: #{captured.inspect}"
      )
    end
  end

  def test_runs_excluded_query_outside_transaction
    fake_pool do |pool|
      Pgtk::Impatient.new(pool, 1, /^VACUUM/).exec('VACUUM book')
    end
  end

  def test_does_not_leave_orphan_backend_after_timeout
    fake_pool(2) do |pool|
      tag = SecureRandom.hex(8)
      sql = "SELECT pg_sleep(30) /* #{tag} */"
      assert_raises(Pgtk::Impatient::TooSlow) do
        Pgtk::Impatient.new(pool, 0.3).exec(sql)
      end
      rows = pool.exec(
        "SELECT pid, state, query FROM pg_stat_activity WHERE query LIKE '%#{tag}%' AND pid <> pg_backend_pid()"
      )
      assert_empty(rows, "orphan backend still running on the server: #{rows.inspect}")
    end
  end

  def test_accepts_fractional_timeout_in_transaction
    fake_pool do |pool|
      captured = []
      spy = Pgtk::Spy.new(pool) { |sql, _| captured << sql }
      Pgtk::Impatient.new(spy, 0.5).transaction do |t|
        t.exec('SELECT 1')
      end
      assert(
        captured.any? { |s| s.match?(/SET LOCAL statement_timeout\s*=\s*500\b/) },
        "must convert fractional seconds to integer milliseconds, got: #{captured.inspect}"
      )
    end
  end
end
