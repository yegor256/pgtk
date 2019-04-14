# frozen_string_literal: true

# Copyright (c) 2019 Yegor Bugayenko
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
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

require 'English'
require 'rake'
require 'rake/tasklib'
require 'shellwords'
require 'yaml'
require_relative '../pgtk'

# Liquibase rake task.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019 Yegor Bugayenko
# License:: MIT
class Pgtk::LiquibaseTask < Rake::TaskLib
  attr_accessor :name
  attr_accessor :master
  attr_accessor :yaml
  attr_accessor :quiet

  def initialize(*args, &task_block)
    @name = args.shift || :liquibase
    unless ::Rake.application.last_description
      desc 'Deploy Liquibase changes to the running PostgreSQL server'
    end
    task(name, *args) do |_, task_args|
      RakeFileUtils.send(:verbose, true) do
        yield(*[self, task_args].slice(0, task_block.arity)) if block_given?
        run
      end
    end
  end

  private

  def run
    yml = YAML.load_file(@yaml)
    raise "YAML at #{yaml} is missing 'pgsql' section" unless yml['pgsql']
    pom = File.expand_path(File.join(__dir__, '../resources/pom.xml'))
    Dir.chdir(File.dirname(@master)) do
      system(
        [
          'mvn verify',
          '--errors',
          @quiet ? '--quiet' : '',
          '--file',
          Shellwords.escape(pom),
          '--define',
          "liquibase.changeLogFile=#{@master}",
          '--define',
          "liquibase.url=#{Shellwords.escape(yml['pgsql']['url'])}",
          '--define',
          "liquibase.logging=#{@quiet ? 'severe' : 'info'}",
          '2>&1'
        ].join(' ')
      )
    end
    raise unless $CHILD_STATUS.exitstatus.zero?
  end
end
