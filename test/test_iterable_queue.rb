# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'minitest/autorun'
require_relative '../lib/pgtk/pool/iterable_queue'

# Test for IterableQueue.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class TestIterableQueue < Minitest::Test
  def test_push_grows_queue_until_full
    q = Pgtk::Pool::IterableQueue.new(2, 5)
    q.push('connA')
    q.push('connB')
    assert_equal(%w[connA connB], q.map { |x| x })
    assert_equal(2, q.size)
  end

  def test_returns_item_to_its_own_slot
    q = Pgtk::Pool::IterableQueue.new(2, 5)
    q.push('connA')
    q.push('connB')
    assert_equal('connA', q.pop)
    q.push('connA')
    assert_equal('connB', q.pop)
    q.push('connB')
    assert_equal(%w[connA connB], q.map { |x| x })
  end

  def test_renewed_push_takes_replaced_slot
    q = Pgtk::Pool::IterableQueue.new(2, 5)
    q.push('connA')
    q.push('connB')
    q.pop
    q.push('connB-renewed', replaced: q.pop)
    assert_equal(
      %w[connA connB-renewed], q.map { |x| x },
      'renewed item must land in the slot of the item it replaced, not evict a foreign one'
    )
    q.push('connA')
    assert_equal(%w[connA connB-renewed], q.map { |x| x })
    assert_equal(2, q.size)
  end

  def test_unknown_push_refuses_to_steal_slot
    q = Pgtk::Pool::IterableQueue.new(2, 5)
    q.push('connA')
    q.push('connB')
    q.pop
    q.pop
    assert_raises(StandardError) { q.push('stranger') }
    assert_equal(%w[connA connB], q.map { |x| x }, 'rejected push must not touch slots that other threads still hold')
    q.push('connA')
    q.push('connB')
    assert_equal(2, q.size)
  end

  def test_unknown_push_grows_when_not_full
    q = Pgtk::Pool::IterableQueue.new(2, 5)
    q.push('connA')
    q.pop
    q.push('connB')
    assert_equal(%w[connA connB], q.map { |x| x })
  end

  def test_renew_push_in_threads_keeps_slots
    q = Pgtk::Pool::IterableQueue.new(2, 5)
    q.push('connA')
    q.push('connB')
    taken = Queue.new
    renewed = Queue.new
    box = Queue.new
    Array.new(2) do |i|
      Thread.new do
        if i.zero?
          box << q.pop
          taken << true
          renewed.pop
          q.push(box.pop)
        else
          taken.pop
          q.push('connB-renewed', replaced: q.pop)
          renewed << true
        end
      end
    end.each(&:join)
    assert_equal(
      %w[connA connB-renewed], q.map { |x| x },
      'both live connections must survive a concurrent renew push'
    )
  end

  def test_raises_busy_when_nothing_free_in_time
    q = Pgtk::Pool::IterableQueue.new(1, 0.05)
    q.push('connA')
    assert_equal('connA', q.pop)
    assert_raises(Pgtk::Pool::Busy) { q.pop }
  end
end
