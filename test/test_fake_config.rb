# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'rake'
require_relative 'test__helper'

# Tests for the +fake_config+ helper itself.
#
# This regression suite documents the root cause of the flakiness reported
# in https://github.com/yegor256/pgtk/issues/217. The helper used to derive
# the Rake task name from a microsecond-resolution timestamp, which can
# collide between two tests in the same process. When the names collide,
# the second +Rake::Task#invoke+ becomes a no-op (because +@already_invoked+
# is already +true+) and the YAML config file is silently never created,
# causing +assert_path_exists+ inside +fake_config+ to fail.
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class TestFakeConfig < Pgtk::Test
  def test_creates_yaml_when_called_twice_with_same_id
    id = "fixed_#{rand(1_000_000)}"
    fake_config(id: id) { |f| assert_path_exists(f) }
    fake_config(id: id) { |f| assert_path_exists(f) }
  end
end
