# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require_relative '../../pgtk'
require_relative '../pool'
require_relative 'busy'

# Thread-safe queue implementation that supports iteration.
# Unlike Ruby's Queue class, this implementation allows safe iteration
# over all elements while maintaining thread safety for concurrent access.
#
# This class is used internally by Pool to store database connections
# and provide the ability to iterate over them for inspection purposes.
#
# The queue is bounded by size. When an item is taken out, it remains in
# the internal array but is marked as "taken". When returned, it's placed
# back in its original slot and marked as available.
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class Pgtk::Pool::IterableQueue
  def initialize(size, timeout)
    @size = size
    @timeout = timeout
    @items = []
    @taken = []
    @free = []
    @mutex = Mutex.new
    @condition = ConditionVariable.new
  end

  def push(item)
    @mutex.synchronize do
      if @items.size < @size
        @items << item
        @taken << false
        @free << (@items.size - 1)
      else
        index = @items.index(item)
        if index.nil?
          index = @taken.index(true)
          raise(StandardError, 'No taken slot found') if index.nil?
          @items[index] = item
        end
        @taken[index] = false
        @free << index
      end
      @condition.signal
    end
  end

  def pop
    @mutex.synchronize do
      deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + @timeout
      while @free.empty?
        remaining = deadline - Process.clock_gettime(Process::CLOCK_MONOTONIC)
        if remaining <= 0
          raise(Pgtk::Pool::Busy, "No free connection appeared in the pool after #{@timeout}s of waiting")
        end
        @condition.wait(@mutex, remaining)
      end
      index = @free.shift
      @taken[index] = true
      @items[index]
    end
  end

  def size
    @mutex.synchronize do
      @items.size
    end
  end

  def map(&)
    @mutex.synchronize do
      @items.map(&)
    end
  end
end
