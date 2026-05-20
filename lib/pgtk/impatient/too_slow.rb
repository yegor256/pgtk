# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require_relative '../../pgtk'
require_relative '../impatient'

# Raised by Pgtk::Impatient#exec when the query takes longer than the
# configured timeout. The deadline is enforced server-side via
# +SET LOCAL statement_timeout+, so the underlying +PG::QueryCanceled+
# is translated into this error for the caller.
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class Pgtk::Impatient::TooSlow < StandardError; end
