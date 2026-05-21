# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'tago'
require_relative '../../pgtk'

class Pgtk::Pool; end

# A temporary class to execute a single SQL request.
#
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class Pgtk::Pool::Txn
  def initialize(conn, log)
    @conn = conn
    @log = log
  end

  # Exec a single parameterized command.
  # @param [String] query The SQL query with params inside (possibly)
  # @param [Array] args List of arguments
  # @param [Integer] result Should be 0 for text results, 1 for binary
  # @yield [Hash] Rows
  def exec(query, args = [], result = 0)
    start = Time.now
    sql = query.is_a?(Array) ? query.join(' ') : query
    @conn.instance_variable_set(:@pgtk_last_query, sql)
    @conn.instance_variable_set(:@pgtk_started_at, start)
    begin
      out =
        if args.empty?
          @conn.exec(sql) do |res|
            if block_given?
              yield(res)
            else
              res.each.to_a
            end
          end
        else
          @conn.exec_params(sql, args, result) do |res|
            if block_given?
              yield(res)
            else
              res.each.to_a
            end
          end
        end
    rescue StandardError => e
      @log.error("#{sql} -> #{e.message}")
      raise(e)
    end
    if (Time.now - start) < 1
      @log.debug("#{sql} >> #{start.ago} / #{@conn.object_id}")
    else
      @log.info("#{sql} >> #{start.ago}")
    end
    out
  end
end
