# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require_relative '../../pgtk'

class Pgtk::Retry; end

# Raised when all retry attempts have been exhausted. The original
# exception that caused the last failure is available via #cause,
# so its message and stack trace are preserved for debugging.
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class Pgtk::Retry::Exhausted < StandardError; end
