# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'English'
require 'qbash'
require 'rake'
require 'rake/tasklib'
require 'shellwords'
require 'yaml'
require_relative '../pgtk'

# Liquibase rake task.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2025 Yegor Bugayenko
# License:: MIT
class Pgtk::LiquibaseTask < Rake::TaskLib
  attr_accessor :name, :master, :yaml, :quiet, :liquibase_version, :postgresql_version

  def initialize(*args, &task_block)
    super()
    @name = args.shift || :liquibase
    @quiet = false
    @liquibase_version = '3.2.2'
    @postgresql_version = '42.7.0'
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
    yml = @yaml
    unless yml.is_a?(Hash)
      yml = YAML.load_file(
        if @yaml.is_a?(Array)
          @yaml.drop_while { |f| !File.exist?(f) }.first
        else
          @yaml
        end
      )
    end
    raise "YAML configuration is missing the 'pgsql' section" unless yml['pgsql']
    @master = File.expand_path(@master)
    unless File.exist?(@master)
      raise \
        "Liquibase master is absent at '#{@master}'. " \
        'More about this file you can find in Liquibase documentation: ' \
        'https://docs.liquibase.com/concepts/changelogs/xml-format.html'
    end
    pom = File.expand_path(File.join(__dir__, '../../resources/pom.xml'))
    old = @liquibase_version.match?(/^[1-3]\..+$/)
    url = yml['pgsql']['url']
    raise "The 'url' is not set in the config (YAML)" if url.nil?
    password = yml['pgsql']['password']
    raise "The 'password' is not set in the config (YAML)" if password.nil?
    Dir.chdir(File.dirname(@master)) do
      qbash(
        [
          'mvn', 'verify',
          '--errors',
          '--batch-mode',
          '--fail-fast',
          @quiet ? '--quiet' : '--debug',
          '--file',
          Shellwords.escape(pom),
          '--define',
          "liquibase.version=#{@liquibase_version}",
          '--define',
          "postgresql.version=#{@postgresql_version}",
          '--define',
          Shellwords.escape("liquibase.searchPath=#{File.dirname(@master)}"),
          '--define',
          Shellwords.escape("liquibase.changeLogFile=#{old ? @master : File.basename(@master)}"),
          '--define',
          Shellwords.escape("liquibase.url=#{url}"),
          '--define',
          Shellwords.escape("liquibase.password=#{password}")
        ]
      )
    end
  end
end
