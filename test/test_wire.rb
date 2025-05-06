# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require_relative 'test__helper'
require_relative '../lib/pgtk/wire'

# Wire test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2025 Yegor Bugayenko
# License:: MIT
class TestWire < Pgtk::Test
  def test_connects
    fake_config do |f|
      wire = Pgtk::Wire::Yaml.new(f)
      c = wire.connection
      refute_nil(c)
    end
  end
end
