# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'pg'
require 'loog'
require_relative '../pgtk'
require_relative 'wire'

# A pool tha spies on another pool.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2025 Yegor Bugayenko
# License:: MIT
class Pgtk::Spy
  def initialize(pool, &block)
    @pool = pool
    @block = block
  end

  def version
    @pool.version
  end

  def exec(sql, *args)
    @block&.call(sql.is_a?(Array) ? sql.join(' ') : sql)
    @pool.exec(sql, *args)
  end

  def transaction
    @pool.transaction do |t|
      yield Pgtk::Spy.new(t, &@block)
    end
  end
end
