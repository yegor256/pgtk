# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'timeout'
require_relative '../pgtk'
require_relative 'wire'

# An impatient decorator of a pool — it doesn't wait too long on every
# request, but terminates them and fails.
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2025 Yegor Bugayenko
# License:: MIT
class Pgtk::Impatient
  def initialize(pool, timeout = 1)
    @pool = pool
    @timeout = timeout
  end

  def version
    @pool.version
  end

  def exec(sql, *args)
    Timeout.timeout(@timeout) do
      @pool.exec(sql, *args)
    end
  end

  def transaction
    @pool.transaction do |t|
      yield Pgtk::Impatient.new(t, @timeout)
    end
  end
end
