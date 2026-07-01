# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'threads'
require 'timeout'
require_relative '../lib/pgtk/pool'
require_relative '../lib/pgtk/stash'
require_relative 'test__helper'

# Pool decorator that calls a block right after every exec,
# used by tests to deterministically inject a concurrent MOD
# into the window between @pool.exec and the stash write lock.
class HookedPool
  def initialize(pool, hook)
    @pool = pool
    @hook = hook
  end

  def start!
    @pool.start!
  end

  def version
    @pool.version
  end

  def dump
    @pool.dump
  end

  def transaction(&)
    @pool.transaction(&)
  end

  def exec(query, params = [], result = 0)
    @pool.exec(query, params, result).tap { @hook.call(query) }
  end
end

# Pool test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2026 Yegor Bugayenko
# License:: MIT
class TestStash < Pgtk::Test
  def test_simple_insert
    fake_pool do |pool|
      assert_predicate(
        Integer(
          Pgtk::Stash.new(pool).exec(
            'INSERT INTO book (title) VALUES ($1) RETURNING id',
            ['Elegant Objects']
          )[0]['id'], 10
        ), :positive?
      )
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

  def test_select_with_keyword_in_string
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.exec('DROP TABLE IF EXISTS tmp CASCADE')
      stash.exec('CREATE TABLE tmp (id INT, title TEXT)')
      stash.exec("INSERT INTO tmp VALUES (1, 'COMMIT or ROLLBACK')")
      first = stash.exec("SELECT * FROM tmp WHERE title LIKE '%COMMIT%'")
      second = stash.exec("SELECT * FROM tmp WHERE title LIKE '%COMMIT%'")
      assert_equal(first.to_a, second.to_a)
      assert_same(first, second, 'SELECT with COMMIT in string must be cached')
    end
  end

  def test_pg_read_function_not_modify
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      pool.exec('DROP TABLE IF EXISTS tmp CASCADE')
      pool.exec('CREATE TABLE tmp (id INT)')
      pool.exec('INSERT INTO tmp VALUES (1)')
      result = stash.exec("SELECT pg_table_size('tmp')")
      refute_nil(result)
      stash.exec('INSERT INTO tmp VALUES (2)')
      refute_same(result, stash.exec("SELECT pg_table_size('tmp')"), 'pg_*() result must not be cached')
    end
  end

  def test_caching
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      query = 'SELECT count(*) FROM book'
      first = stash.exec(query)
      second = stash.exec(query)
      assert_equal(first.to_a, second.to_a)
      assert_same(first, second)
    end
  end

  def test_cache_invalidation
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      query = 'SELECT count(*) FROM book'
      stash.exec(query).then do |first|
        stash.exec('INSERT INTO book (title) VALUES ($1)', ['New Book'])
        refute_same(first, stash.exec(query))
      end
    end
  end

  def test_cte_write_invalidates_cache
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['Old Title'])
      query = 'SELECT title FROM book'
      first = stash.exec(query)
      assert_same(first, stash.exec(query), 'SELECT must be cached before the CTE write')
      stash.exec(
        [
          'WITH doomed AS (SELECT id FROM book WHERE title = $1)',
          'DELETE FROM book USING doomed WHERE book.id = doomed.id RETURNING book.title'
        ].join(' '),
        ['Old Title']
      )
      refute_same(
        first, stash.exec(query),
        'a data-modifying CTE (WITH ... DELETE) must invalidate a previously-cached SELECT on the same table'
      )
      assert_empty(stash.exec(query).to_a, 'the deleted row must be gone from the cached SELECT')
    end
  end

  def test_cte_read_stays_cached
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['Readable'])
      query = 'WITH recent AS (SELECT title FROM book) SELECT title FROM recent'
      assert_same(
        stash.exec(query), stash.exec(query),
        'a read-only CTE (WITH ... SELECT) must still be cached as a read'
      )
    end
  end

  def test_invalidates_cache_for_underscored_table
    fake_pool do |pool|
      pool.exec('CREATE TABLE user_settings (id INTEGER PRIMARY KEY, value TEXT NOT NULL)')
      stash = Pgtk::Stash.new(pool)
      query = 'SELECT value FROM user_settings WHERE id = $1'
      stash.exec(query, [1]).then do |first|
        stash.exec('INSERT INTO user_settings (id, value) VALUES ($1, $2)', [1, 'x'])
        refute_same(
          first, stash.exec(query, [1]),
          'cannot invalidate cache for a write into a table whose name has an underscore'
        )
      end
    end
  end

  def test_invalidates_cache_for_digited_table
    fake_pool do |pool|
      pool.exec('CREATE TABLE audit_log_2024 (id INTEGER PRIMARY KEY, msg TEXT NOT NULL)')
      stash = Pgtk::Stash.new(pool)
      query = 'SELECT msg FROM audit_log_2024 WHERE id = $1'
      stash.exec(query, [1]).then do |first|
        stash.exec('INSERT INTO audit_log_2024 (id, msg) VALUES ($1, $2)', [1, 'x'])
        refute_same(
          first, stash.exec(query, [1]),
          'cannot invalidate cache for a write into a table whose name has a digit'
        )
      end
    end
  end

  def test_caches_select_from_digited_table
    fake_pool do |pool|
      pool.exec('CREATE TABLE audit_log_2024 (id INTEGER PRIMARY KEY, msg TEXT NOT NULL)')
      stash = Pgtk::Stash.new(pool)
      query = 'SELECT msg FROM audit_log_2024 WHERE id = $1'
      assert_same(
        stash.exec(query, [1]), stash.exec(query, [1]),
        'cannot cache a SELECT from a table whose name has a digit'
      )
    end
  end

  def test_caching_with_params
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      query = 'SELECT * FROM book WHERE title = $1'
      first = stash.exec(query, ['Elegant Objects'])
      second = stash.exec(query, ['Elegant Objects'])
      assert_equal(first.to_a, second.to_a)
      assert_same(first, second)
      refute_same(first, stash.exec(query, ['Different Title']))
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
      assert_raises(ArgumentError) { stash.exec('SELECT 1;') }
      assert_raises(ArgumentError) { stash.exec('SELECT * FROM generate_series(1, 5);') }
    end
  end

  def test_version
    fake_pool do |pool|
      assert_match(/^\d+\.\d+/, Pgtk::Stash.new(pool).version)
    end
  end

  def test_transaction
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['Transaction Test'])
      stash.transaction do |tx|
        assert_equal(
          'Transaction Test',
          tx.exec('SELECT title FROM book WHERE title = $1', ['Transaction Test'])[0]['title']
        )
        true
      end
    end
  end

  def test_start
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['Start Test'])
      stash.start!
      assert_equal('Start Test', stash.exec('SELECT title FROM book WHERE title = $1', ['Start Test'])[0]['title'])
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
    interval = 0.2
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, refill: interval)
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['My book'])
      stash.exec('SELECT id, title FROM book WHERE title = $1 ORDER BY id DESC', ['My book'])
      stash.exec('SELECT id, title FROM book WHERE title = $1 ORDER BY id DESC', ['My book'])
      stash.exec('SELECT id, title FROM book WHERE title = $1 ORDER BY id DESC', ['My book'])
      assert_includes(stash.dump, '1/3p/0s/')
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['My book'])
      assert_includes(stash.dump, '1/3p/1s/')
      sleep(interval * 2)
      assert_includes(stash.dump, '1/3p/0s/')
    end
  end

  def test_cache_refill_respects_pause
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, refill: 0.1, delay: 4)
      stash.start!
      stash.exec('SELECT * FROM book')
      assert_includes(stash.dump, '1/1p/0s/')
      stash.exec("INSERT INTO book (title) VALUES ('Elegant Objects')")
      assert_includes(stash.dump, '1/1p/1s/')
      sleep(0.2)
      assert_includes(stash.dump, '1/1p/1s/')
    end
  end

  def test_caps_oldest_queries
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, cap: 1, capping: 0.1)
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['1984'])
      5.times do |i|
        stash.exec('SELECT * FROM book WHERE id = $1', [i])
        sleep(0.01)
      end
      stash.exec('SELECT id FROM book')
      sleep(0.01)
      stash.exec('SELECT title FROM book')
      sleep(0.2)
      assert_includes(stash.dump, '1/1p/0s')
    end
  end

  def test_retire_oldest_queries
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, retire: 0.1, capping: 0.1)
      stash.start!
      stash.exec('SELECT * FROM book WHERE id = $1', [1])
      assert_includes(stash.dump, '1/1p/0s')
      sleep(0.3)
      assert_includes(stash.dump, '1/1p/0s')
    end
  end

  def test_capper_prunes_tables_index
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, cap: 1, capping: 0.1, refill: nil, retirement: nil)
      stash.start!
      stash.exec('SELECT id FROM book')
      sleep(0.02)
      stash.exec('SELECT title FROM book')
      sleep(0.4)
      inner = stash.instance_variable_get(:@stash)
      assert_equal(
        inner[:queries].keys.sort, (inner[:tables]['book'] || []).sort,
        'tables index must drop query strings evicted by the cap'
      )
    end
  end

  def test_retiree_prunes_tables_index
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, retire: 0.1, retirement: 0.1, refill: nil, capping: nil)
      stash.start!
      stash.exec('SELECT id FROM book')
      stash.exec('SELECT title FROM book')
      sleep(0.5)
      assert_empty(
        stash.instance_variable_get(:@stash)[:tables],
        'tables index must drop query strings retired from cache'
      )
    end
  end

  def test_does_not_cache_current_timestamp
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['My book'])
      stash.exec('SELECT CURRENT_TIMESTAMP, title FROM book WHERE id = $1', [1])
      refute_includes(
        stash.dump, 'CURRENT_TIMESTAMP',
        'CURRENT_TIMESTAMP is non-deterministic and must not be cached'
      )
    end
  end

  def test_does_not_cache_random
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['My book'])
      stash.exec('SELECT RANDOM(), title FROM book WHERE id = $1', [1])
      refute_includes(stash.dump, 'RANDOM', 'RANDOM() is non-deterministic and must not be cached')
    end
  end

  def test_does_not_cache_gen_random_uuid
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['My book'])
      stash.exec('SELECT GEN_RANDOM_UUID(), title FROM book WHERE id = $1', [1])
      refute_includes(stash.dump, 'GEN_RANDOM_UUID', 'GEN_RANDOM_UUID() is non-deterministic and must not be cached')
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
      assert_includes(stash.dump, 'queries in cache')
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
  def test_preserves_stale_marker_on_refill
    fake_pool do |real_pool|
      deleted = false
      triggered = false
      stash = nil
      stash = Pgtk::Stash.new(
        HookedPool.new(
          real_pool,
          lambda do |q|
            next unless deleted && !triggered && q.start_with?('SELECT')
            triggered = true
            stash.exec('INSERT INTO book (title) VALUES ($1)', ['B'])
          end
        )
      )
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['A'])
      stash.exec('SELECT title FROM book')
      stash.exec('DELETE FROM book WHERE title = $1', ['A'])
      deleted = true
      stash.exec('SELECT title FROM book')
      assert_includes(
        stash.exec('SELECT title FROM book').to_a.map do |r|
          r['title']
        end, 'B', 'cannot see row inserted during a concurrent stash refill'
      )
    end
  end

  def test_refill_does_not_clobber_fresh_cache
    fake_pool do |real_pool|
      gate = Concurrent::Event.new
      release = Concurrent::Event.new
      armed = Concurrent::AtomicBoolean.new(false)
      stash = Pgtk::Stash.new(
        HookedPool.new(
          real_pool,
          lambda do |q|
            next unless armed.value && q.include?('SELECT title FROM book')
            armed.make_false
            gate.set
            release.wait(10)
          end
        ),
        refill: nil, capping: nil, retirement: nil, delay: 0
      )
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['A'])
      stash.exec('SELECT title FROM book')
      stash.exec('DELETE FROM book WHERE title = $1', ['A'])
      armed.make_true
      stash.__send__(:replenish, 'SELECT title FROM book')
      raise(Timeout::Error, 'hook gate never reached') unless gate.wait(10)
      stash.exec('SELECT title FROM book')
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['B'])
      stash.exec('SELECT title FROM book')
      release.set
      tpool = stash.instance_variable_get(:@tpool)
      tpool.shutdown
      tpool.wait_for_termination(10)
      assert_includes(
        stash.exec('SELECT title FROM book').to_a.map do |r|
          r['title']
        end, 'B', 'refill task clobbered fresh cache with its stale snapshot'
      )
    end
  end

  def test_refill_skips_when_modify_in_flight
    fake_pool do |real_pool|
      selected = Concurrent::Event.new
      committed = Concurrent::Event.new
      applied = Concurrent::Event.new
      armed = Concurrent::AtomicBoolean.new(false)
      stash = Pgtk::Stash.new(
        HookedPool.new(
          real_pool,
          lambda do |q|
            next unless armed.value
            if q.include?('SELECT title FROM book')
              selected.set
              raise(Timeout::Error, 'modify never committed') unless committed.wait(10)
            elsif q.start_with?('INSERT INTO book')
              committed.set
              raise(Timeout::Error, 'refill never finished') unless applied.wait(10)
            end
          end
        ),
        refill: nil, capping: nil, retirement: nil, delay: 0
      )
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['A'])
      stash.exec('SELECT title FROM book')
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['B'])
      armed.make_true
      Thread.new do
        raise(Timeout::Error, 'refill SELECT never reached PG') unless selected.wait(10)
        stash.exec('INSERT INTO book (title) VALUES ($1)', ['C'])
      end.tap do
        stash.__send__(:replenish, 'SELECT title FROM book')
        tpool = stash.instance_variable_get(:@tpool)
        tpool.shutdown
        tpool.wait_for_termination(10)
        armed.make_false
        assert_includes(
          stash.exec('SELECT title FROM book').to_a.map { |r| r['title'] }.tap { applied.set },
          'C', 'refill task cleared :stale with pre-commit data while a modify was in flight'
        )
      end.join
    end
  end

  def test_cold_miss_marks_stale_on_race
    fake_pool do |real_pool|
      triggered = false
      stash = nil
      stash = Pgtk::Stash.new(
        HookedPool.new(
          real_pool,
          lambda do |q|
            next if triggered
            next unless q.start_with?('SELECT title FROM book')
            triggered = true
            stash.exec('INSERT INTO book (title) VALUES ($1)', ['B'])
          end
        )
      )
      stash.exec('SELECT title FROM book')
      assert_includes(
        stash.exec('SELECT title FROM book').to_a.map do |r|
          r['title']
        end, 'B', 'cannot see row inserted during a cold-miss SELECT'
      )
    end
  end

  def test_two_writes_same_tick_marks_cache_stillborn
    fake_pool do |real_pool|
      triggered = false
      stash = nil
      frozen = Time.now
      stash = Pgtk::Stash.new(
        HookedPool.new(
          real_pool,
          lambda do |q|
            next if triggered
            next unless q.start_with?('SELECT title FROM book')
            triggered = true
            stash.exec('INSERT INTO book (title) VALUES ($1)', ['B'])
          end
        ),
        refill: nil, capping: nil, retirement: nil
      )
      Time.stub(:now, frozen) do
        stash.exec('INSERT INTO book (title) VALUES ($1)', ['A'])
      end
      Time.stub(:now, frozen) do
        stash.exec('SELECT title FROM book')
      end
      assert_includes(
        stash.exec('SELECT title FROM book').map { |r| r['title'] },
        'B',
        'must detect stillborn cache when two writes share the same clock tick'
      )
    end
  end

  def test_replenish_survives_eviction
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, refill: nil, capping: nil, retirement: nil, delay: 0)
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['A'])
      stash.exec('SELECT title FROM book')
      stash.exec('DELETE FROM book WHERE title = $1', ['A'])
      query = 'SELECT title FROM book'
      target = stash.instance_variable_get(:@stash)[:queries][query]
      target.define_singleton_method(:keys) do
        snapped = super()
        snapped.each { |k| delete(k) }
        snapped
      end
      stash.__send__(:replenish, query)
      tpool = stash.instance_variable_get(:@tpool)
      tpool.shutdown
      tpool.wait_for_termination(10)
      assert(
        stash.instance_variable_get(:@stash)[:queries][query]&.values&.none? { |h| h[:stale] },
        'replenish must not crash or leave entries stale when keys vanish between snapshot and access'
      )
    end
  end

  def test_concurrent_stash_iteration_is_safe
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, threads: 1, refill: 1)
      stash.start!
      Threads.new(10).assert(10) do
        sleep(0.25)
        10.times do
          stash.exec('INSERT INTO book (title) VALUES ($1)', ['My book'])
          stash.exec('SELECT id, title FROM book WHERE id = $1', [rand(1..100)])
          stash.exec("SELECT id, title FROM book WHERE id = $1 OR 1 = #{rand(1..100)}", [rand(1..100)])
          sleep(0.25)
        end
        sleep(0.25)
      end
    end
  end

  def test_select_stays_consistent_under_writers
    fake_pool(8) do |pool|
      pool.exec('CREATE TABLE node (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)')
      stash = Pgtk::Stash.new(pool, refill: 0.3, delay: 4, capping: 5, retirement: 5, retire: 600)
      stash.start!
      hammer(stash, 16, 2, 4, 10)
      sleep(stash.instance_variable_get(:@refill) + stash.instance_variable_get(:@delay) + 2)
      assert_empty(diverged(stash, pool, 16), 'stash diverged from DB after settled load')
    end
  end

  def test_list_query_converges_after_writers
    fake_pool(8) do |pool|
      pool.exec('CREATE TABLE node (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)')
      stash = Pgtk::Stash.new(pool, refill: 0.3, delay: 4, capping: 5, retirement: 5, retire: 600)
      stash.start!
      hammer(stash, 16, 2, 4, 10)
      sleep(stash.instance_variable_get(:@refill) + stash.instance_variable_get(:@delay) + 2)
      assert_empty(ghosts(stash, pool), 'list query keeps returning ghost ids whose rows no longer exist in DB')
    end
  end

  def test_cache_hit_does_not_block_on_reader
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, refill: nil, capping: nil, retirement: nil)
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['Elegant Objects'])
      query = 'SELECT id, title FROM book WHERE id = $1'
      stash.exec(query, [1])
      Thread.new { stash.instance_variable_get(:@entrance).with_read_lock { sleep(3) } }
      sleep(0.5)
      assert_equal(
        'Elegant Objects',
        Timeout.timeout(2) { stash.exec(query, [1]) }.first['title'],
        'a cache hit must not serialize through the writer while a reader holds the lock'
      )
    end
  end

  def test_cache_hit_bumps_popularity
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, refill: nil, capping: nil, retirement: nil)
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['Elegant Objects'])
      query = 'SELECT id, title FROM book WHERE id = $1'
      5.times { stash.exec(query, [1]) }
      assert_equal(
        5,
        stash.instance_variable_get(:@stash)[:queries][query].values.first[:popularity].value,
        'every cache hit must bump popularity'
      )
    end
  end

  def test_cached_count_does_not_block_on_reader
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, refill: nil, capping: nil, retirement: nil)
      stash.start!
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['Elegant Objects'])
      stash.exec('SELECT id, title FROM book WHERE id = $1', [1])
      Thread.new { stash.instance_variable_get(:@entrance).with_read_lock { sleep(3) } }
      sleep(0.5)
      assert_includes(
        Timeout.timeout(2) { stash.dump },
        'queries cached',
        'counting cached entries must take the read lock, not the writer'
      )
    end
  end

  def test_select_does_not_clear_stale_marker
    fake_pool do |pool|
      stash = Pgtk::Stash.new(pool, refill: nil, capping: nil, retirement: nil)
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['A'])
      stash.exec('SELECT title FROM book')
      stash.exec('INSERT INTO book (title) VALUES ($1)', ['B'])
      stash.exec('SELECT title FROM book')
      refute_nil(
        stash.instance_variable_get(:@stash)[:queries]['SELECT title FROM book'].values.first[:stale],
        'cache must not clear the stale marker; only replenish should clear it'
      )
    end
  end

  def test_cascade_delete_invalidates_child_cache
    fake_pool do |pool|
      pool.exec('CREATE TABLE org (id INTEGER PRIMARY KEY)')
      pool.exec(<<~SQL)
        CREATE TABLE node (
          id INTEGER PRIMARY KEY,
          org_id INTEGER REFERENCES org(id) ON DELETE CASCADE,
          ip TEXT NOT NULL
        )
      SQL
      stash = Pgtk::Stash.new(pool, refill: nil, capping: nil, retirement: nil)
      stash.start!
      stash.exec('INSERT INTO org (id) VALUES ($1)', [1])
      stash.exec('INSERT INTO node (id, org_id, ip) VALUES ($1, $2, $3)', [42, 1, '10.0.0.1'])
      stash.exec('SELECT ip FROM node WHERE id = $1', [42])
      stash.exec('DELETE FROM org WHERE id = $1', [1])
      assert_nil(
        stash.exec('SELECT ip FROM node WHERE id = $1', [42]).first,
        'cannot serve cached node row after parent org was cascade-deleted'
      )
    end
  end

  def test_inflight_under_concurrent_modifies
    fake_pool(4) do |pool|
      stash = Pgtk::Stash.new(pool)
      stash.start!
      Array.new(4) do
        Thread.new do
          10.times do
            stash.exec('UPDATE book SET title = $2 WHERE title = $1', [SecureRandom.hex(4), SecureRandom.hex(4)])
          end
        end
      end.each(&:join)
      assert_equal(
        0, stash.instance_variable_get(:@stash)[:table_inflight]['book']&.value,
        'inflight must be 0 after concurrent modifies'
      )
    end
  end

  private

  def hammer(stash, count, writers, readers, seconds)
    stop = Concurrent::AtomicBoolean.new(false)
    crashes = Concurrent::Array.new
    (
      Array.new(writers) { Thread.new { spam(stash, count, stop, crashes) } } +
      Array.new(readers) { Thread.new { scan(stash, stop, crashes) } }
    ).tap do
      sleep(seconds)
      stop.make_true
    end.each(&:join)
    assert_empty(crashes, "thread crashed: #{crashes.first}")
  end

  def spam(stash, count, stop, crashes)
    until stop.value
      id = rand(count)
      begin
        stash.exec('INSERT INTO node (id, payload) VALUES ($1, $2)', [id, "p-#{id}-#{rand(1_000_000)}"])
        stash.exec('DELETE FROM node WHERE id = $1', [id])
      rescue PG::UniqueViolation
        next
      end
    end
  rescue StandardError => e
    crashes << "writer: #{e.class}: #{e.message}"
  end

  def scan(stash, stop, crashes)
    until stop.value
      ids = stash.exec('SELECT id FROM node ORDER BY id').map { |r| Integer(r['id'], 10) }
      ids.each { |id| stash.exec('SELECT payload FROM node WHERE id = $1', [id]) }
    end
  rescue StandardError => e
    crashes << "reader: #{e.class}: #{e.message}"
  end

  def diverged(stash, pool, count)
    truth = pool.exec('SELECT id, payload FROM node').to_h { |r| [Integer(r['id'], 10), r['payload']] }
    bugs = []
    count.times do |id|
      cached = stash.exec('SELECT payload FROM node WHERE id = $1', [id]).first
      expected = truth[id]
      if expected.nil?
        bugs << "id=#{id}: stash=#{cached.inspect}, DB has no row" unless cached.nil?
      elsif cached.nil? || cached['payload'] != expected
        bugs << "id=#{id}: stash=#{cached.inspect}, DB=#{expected.inspect}"
      end
    end
    bugs
  end

  def ghosts(stash, pool)
    stash.exec('SELECT id FROM node ORDER BY id').map do |r|
      Integer(r['id'], 10)
    end - pool.exec('SELECT id FROM node').map do |r|
      Integer(r['id'], 10)
    end
  end
end
