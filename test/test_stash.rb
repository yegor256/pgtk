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
end
