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
# Copyright:: Copyright (c) 2019-2024 Yegor Bugayenko
# License:: MIT
class Pgtk::LiquibaseTask < Rake::TaskLib
  attr_accessor :name, :master, :yaml, :quiet

  def initialize(*args, &task_block)
    super()
    @name = args.shift || :liquibase
    @quite = false
    desc 'Deploy Liquibase changes to the running PostgreSQL server' unless ::Rake.application.last_description
    task(name, *args) do |_, task_args|
      RakeFileUtils.send(:verbose, true) do
        yield(*[self, task_args].slice(0, task_block.arity)) if block_given?
        run
      end
    end
  end

  private

  def run
    raise "Option 'master' is mandatory" unless @master
    raise "Option 'yaml' is mandatory" unless @yaml
    yml = YAML.load_file(
      if @yaml.is_a?(Array)
        @yaml.drop_while { |f| !File.exist?(f) }.first
      else
        @yaml
      end
    )
    raise "YAML at #{yaml} is missing 'pgsql' section" unless yml['pgsql']
    pom = File.expand_path(File.join(__dir__, '../../resources/pom.xml'))
    raise "Liquibase master is absent at #{@master}" unless File.exist?(@master)
    @master = File.expand_path(@master)
    Dir.chdir(File.dirname(@master)) do
      system(
        [
          'mvn verify',
          '--errors',
          '--batch-mode',
          @quiet ? '--quiet' : '',
          '--file',
          Shellwords.escape(pom),
          '--define',
          "liquibase.changeLogFile=#{@master}",
          '--define',
          "liquibase.url=#{Shellwords.escape(yml['pgsql']['url'])}",
          '--define',
          "liquibase.password=#{Shellwords.escape(yml['pgsql']['password'])}",
          '--define',
          "liquibase.logging=#{@quiet ? 'severe' : 'info'}",
          '2>&1'
        ].join(' ')
      )
    end
    raise unless $CHILD_STATUS.exitstatus.zero?
  end
end
