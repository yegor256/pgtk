# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require_relative '../../pgtk'

class Pgtk::LiquicheckTask; end

# Internal error raised by Pgtk::LiquicheckTask validation helpers and
# captured per-file by the +on+ helper to accumulate readable diagnostics.
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class Pgtk::LiquicheckTask::MustError < StandardError
end
