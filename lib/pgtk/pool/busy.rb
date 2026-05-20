# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require_relative '../../pgtk'
require_relative '../pool'

# Raised when no connection becomes available from the pool within
# the configured timeout. Indicates that all connections are currently
# taken by other threads and none was returned in time.
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class Pgtk::Pool::Busy < StandardError; end
