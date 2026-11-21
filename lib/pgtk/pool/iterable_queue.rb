# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require_relative '../../pgtk'
require_relative 'busy'

class Pgtk::Pool; end

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

  # Push an item back to the queue, making its slot available again.
  #
  # When the pushed +item+ is a brand-new object that must take the place
  # of an older one (a renewed database connection, for example), pass the
  # object it replaces as +replaced+: the new item then lands exactly in
  # the slot the old one occupied. Without this hint an unknown item cannot
  # be matched to a slot, and the queue refuses it instead of guessing,
  # since guessing could evict an item another thread is still using.
  #
  # @param [Object] item The item to return to the queue
  # @param [Object, nil] replaced The item this one takes the place of, or +nil+
  # @raise [StandardError] If the queue is full and +item+ occupies no slot
  def push(item, replaced: nil)
    @mutex.synchronize do
      index = @items.index(item)
      if index.nil? && replaced
        index = @items.index(replaced)
        @items[index] = item if index
      end
      if index.nil?
        if @items.size >= @size
          raise(StandardError, "The queue is full, while #{item.inspect} does not occupy any slot in it")
        end
        @items << item
        @taken << false
        index = @items.size - 1
      end
      @taken[index] = false
      @free << index
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
