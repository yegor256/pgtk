# frozen_string_literal: true

# Copyright (c) 2019-2024 Yegor Bugayenko
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the 'Software'), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

require 'minitest/autorun'
require 'rake'
require 'tmpdir'
require 'yaml'
require_relative '../lib/pgtk/pgsql_task'

# Pgsql rake task test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2019 Yegor Bugayenko
# License:: MIT
class TestPgsqlTask < Minitest::Test
  def test_basic
    Dir.mktmpdir 'test' do |dir|
      Pgtk::PgsqlTask.new(:p2) do |t|
        t.dir = File.join(dir, 'pgsql')
        t.user = 'hello'
        t.password = 'A B C привет ! & | !'
        t.dbname = 'test'
        t.yaml = File.join(dir, 'cfg.yml')
        t.quiet = true
        t.config = {
          log_min_error_statement: 'ERROR'
        }
      end
      Rake::Task['p2'].invoke
      yaml = YAML.load_file(File.join(dir, 'cfg.yml'))
      assert(yaml['pgsql']['url'].start_with?('jdbc:postgresql://localhost'))
    end
  end

  def test_not_quiet
    Dir.mktmpdir 'test' do |dir|
      Pgtk::PgsqlTask.new(:p3) do |t|
        t.dir = File.join(dir, 'pgsql')
        t.user = 'hello'
        t.password = 'the password'
        t.dbname = 'test'
        t.yaml = File.join(dir, 'cfg.yml')
        t.quiet = false
        t.config = {
          log_directory: dir,
          logging_collector: 'on',
          log_statement: 'all',
          log_filename: 'pgsql.log'
        }
      end
      Rake::Task['p3'].invoke
      yaml = YAML.load_file(File.join(dir, 'cfg.yml'))
      assert(yaml['pgsql']['url'].start_with?('jdbc:postgresql://localhost'))
      assert(File.exist?(File.join(dir, 'pgsql.log')))
    end
  end
end
