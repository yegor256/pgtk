# frozen_string_literal: true

require 'English'
# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'donce'
require 'loog'
require 'os'
require 'qbash'
require 'rake'
require 'rake/tasklib'
require 'shellwords'
require 'yaml'
require_relative '../pgtk'

# Liquibase rake task.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class Pgtk::LiquibaseTask < Rake::TaskLib
  attr_accessor :name, :master, :yaml, :schema, :quiet, :liquibase, :postgresql, :contexts, :docker

  # Initialize a new Liquibase task.
  #
  # @param [Array] args Task arguments
  # @yield [Pgtk::LiquibaseTask, Object] Yields self and task arguments
  def initialize(*args, &task_block)
    super()
    @docker ||= :maybe
    @name = args.shift || :liquibase
    @quiet = false
    @contexts = ''
    @liquibase = '3.2.2'
    @postgresql = '42.7.0'
    desc('Deploy Liquibase changes to the running PostgreSQL server') unless ::Rake.application.last_description
    task(name, *args) do |_, task_args|
      RakeFileUtils.verbose(true) do
        yield(*[self, task_args].slice(0, task_block.arity)) if block_given?
        run
      end
    end
  end

  private

  def run
    raise(ArgumentError, "Option 'master' is mandatory") unless @master
    raise(ArgumentError, "Option 'yaml' is mandatory") unless @yaml
    yml = config
    @master = File.expand_path(@master)
    validate(yml)
    migrate(yml)
    dump(yml) if @schema
  end

  def config
    yml = @yaml
    return yml if yml.is_a?(Hash)
    YAML.load_file(
      if @yaml.is_a?(Array)
        @yaml.drop_while { |f| !File.exist?(f) }.first
      else
        @yaml
      end
    )
  end

  def validate(yml)
    raise(ArgumentError, "YAML configuration is missing the 'pgsql' section") unless yml['pgsql']
    unless File.exist?(@master)
      raise(
        ArgumentError,
        "Liquibase master is absent at '#{@master}'. " \
        'More about this file you can find in Liquibase documentation: ' \
        'https://docs.liquibase.com/concepts/changelogs/xml-format.html'
      )
    end
    raise(ArgumentError, "The 'url' is not set in the config (YAML)") if yml['pgsql']['url'].nil?
    raise(ArgumentError, "The 'user' is not set in the config (YAML)") if yml['pgsql']['user'].nil?
    raise(ArgumentError, "The 'password' is not set in the config (YAML)") if yml['pgsql']['password'].nil?
  end

  def migrate(yml)
    pom = File.expand_path(File.join(__dir__, '../../resources/pom.xml'))
    old = @liquibase.match?(/^[1-3]\..+$/)
    url = yml['pgsql']['url']
    username = yml['pgsql']['user']
    password = yml['pgsql']['password']
    Dir.chdir(File.dirname(@master)) do
      qbash(
        'mvn', 'verify',
        '--errors',
        '--batch-mode',
        '--fail-fast',
        @quiet ? '--quiet' : '--debug',
        '--file',
        Shellwords.escape(pom),
        '--define',
        "liquibase.version=#{@liquibase}",
        '--define',
        "postgresql.version=#{@postgresql}",
        '--define',
        Shellwords.escape("liquibase.searchPath=#{File.dirname(@master)}"),
        '--define',
        Shellwords.escape("liquibase.changeLogFile=#{old ? @master : File.basename(@master)}"),
        '--define',
        Shellwords.escape("liquibase.url=#{url}"),
        '--define',
        Shellwords.escape("liquibase.username=#{username}"),
        '--define',
        Shellwords.escape("liquibase.password=#{password}"),
        '--define',
        Shellwords.escape("liquibase.contexts=#{@contexts}"),
        stdout: @quiet ? Loog::NULL : Loog::REGULAR,
        stderr: Loog::REGULAR
      )
    end
  end

  def dump(yml)
    @schema = File.expand_path(@schema)
    local = qbash('pg_dump -V', accept: nil, both: true)[1].zero?
    docker = qbash('docker -v', accept: nil, both: true)[1].zero?
    raise(IOError, 'Cannot generate schema, install either pg_dump or Docker') unless local || docker
    raise(ArgumentError, 'You set docker to :always, but Docker is not installed') if @docker == :always && !docker
    password = yml['pgsql']['password']
    host = yml.dig('pgsql', 'host')
    Dir.chdir(File.dirname(@schema)) do
      out =
        if (local && @docker != :always) || @docker == :never
          pgdump(yml, host, password)
        else
          host = donce_host if OS.mac? && ['localhost', '127.0.0.1'].include?(host)
          dockerdump(yml, host, password)
        end
      File.write(@schema, out)
    end
  end

  def pgdump(yml, host, password)
    qbash(
      'pg_dump',
      '-h', Shellwords.escape(host),
      '-p', Shellwords.escape(yml.dig('pgsql', 'port').to_s),
      '-U', Shellwords.escape(yml.dig('pgsql', 'user')),
      '-d', Shellwords.escape(yml.dig('pgsql', 'dbname')),
      '-n', 'public',
      '--schema-only',
      env: { 'PGPASSWORD' => password },
      stdout: @quiet ? Loog::NULL : Loog::REGULAR,
      stderr: Loog::REGULAR
    )
  end

  def dockerdump(yml, host, password)
    donce(
      image: 'postgres:18.1',
      args: OS.mac? ? '' : '--network=host',
      env: { 'PGPASSWORD' => password },
      command: [
        'pg_dump',
        '-h', host,
        '-p', yml.dig('pgsql', 'port').to_s,
        '-U', yml.dig('pgsql', 'user'),
        '-d', yml.dig('pgsql', 'dbname'),
        '-n', 'public',
        '--schema-only'
      ].shelljoin,
      stdout: @quiet ? Loog::NULL : Loog::REGULAR,
      stderr: Loog::REGULAR
    )
  end
end
