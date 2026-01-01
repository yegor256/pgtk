# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'cgi'
require 'English'
require 'qbash'
require 'rake'
require 'rake/tasklib'
require 'random-port'
require 'shellwords'
require 'securerandom'
require 'tempfile'
require 'yaml'
require 'waitutil'
require_relative '../pgtk'

# Pgsql rake task.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class Pgtk::PgsqlTask < Rake::TaskLib
  # Task name
  # @return [Symbol]
  attr_accessor :name

  # Directory where PostgreSQL server files will be stored
  # @return [String]
  attr_accessor :dir

  # Whether to delete the PostgreSQL data directory on each run
  # @return [Boolean]
  attr_accessor :fresh_start

  # PostgreSQL username
  # @return [String]
  attr_accessor :user

  # PostgreSQL password
  # @return [String]
  attr_accessor :password

  # PostgreSQL database name
  # @return [String]
  attr_accessor :dbname

  # Path to YAML file where configuration will be written
  # @return [String]
  attr_accessor :yaml

  # Whether to suppress output
  # @return [Boolean]
  attr_accessor :quiet

  # TCP port for PostgreSQL server (random if nil)
  # @return [Integer, nil]
  attr_accessor :port

  # Configuration options for PostgreSQL server
  # @return [Hash]
  attr_accessor :config

  # Use force docker
  # @return [Boolean]
  attr_accessor :force_docker

  # Initialize a new PostgreSQL server task.
  #
  # @param [Array] args Task arguments
  # @yield [Pgtk::PgsqlTask, Object] Yields self and task arguments
  def initialize(*args, &task_block)
    super()
    @name = args.shift || :pgsql
    @fresh_start = false
    @quiet = false
    @user = 'test'
    @config = {}
    @password = 'test'
    @dbname = 'test'
    @port = nil
    desc 'Start a local PostgreSQL server' unless ::Rake.application.last_description
    task(name, *args) do |_, task_args|
      RakeFileUtils.send(:verbose, true) do
        yield(*[self, task_args].slice(0, task_block.arity)) if block_given?
        run
      end
    end
  end

  private

  def run
    pg_out = qbash('postgres -V; initdb -V', accept: nil, both: true)
    local = pg_out[1].zero?
    docker_out = qbash('docker -v', accept: nil, both: true)
    docker = docker_out[1].zero?
    unless local || docker
      raise \
        "Failed to find either PostgreSQL or Docker:\n#{pg_out.first}\n#{docker_out.first}"
    end
    raise 'You cannot force Docker to run, because it is not installed locally' if @force_docker && !docker
    raise "Option 'dir' is mandatory" unless @dir
    raise "Option 'yaml' is mandatory" unless @yaml
    home = File.expand_path(@dir)
    FileUtils.rm_rf(home) if @fresh_start
    raise "Directory/file #{home} is present, use fresh_start=true" if File.exist?(home)
    stdout = @quiet ? nil : $stdout
    port = @port
    if port.nil?
      port = RandomPort::Pool::SINGLETON.acquire
      puts "Random TCP port #{port} is used for PostgreSQL server" unless @quiet
    else
      puts "Required TCP port #{port} is used for PostgreSQL server" unless @quiet
    end
    if local && !@force_docker
      pid = run_local(home, stdout, port)
      place = "in process ##{pid}"
    else
      container = run_docker(home, stdout, port)
      place = "in container #{container}"
    end
    File.write(
      @yaml,
      {
        'pgsql' => {
          'host' => 'localhost',
          'port' => port,
          'dbname' => @dbname,
          'user' => @user,
          'password' => @password,
          'url' => [
            "jdbc:postgresql://localhost:#{port}/",
            "#{CGI.escape(@dbname)}?user=#{CGI.escape(@user)}"
          ].join
        }
      }.to_yaml
    )
    return if @quiet
    puts "PostgreSQL has been started #{place}, port #{port}"
    puts "YAML config saved to #{@yaml}"
  end

  def run_docker(home, stdout, port)
    FileUtils.mkdir_p(home)
    out =
      qbash(
        [
          'docker',
          'run',
          "--publish #{Shellwords.escape("#{port}:5432")}",
          "-e POSTGRES_USER=#{Shellwords.escape(@user)}",
          "-e POSTGRES_PASSWORD=#{Shellwords.escape(@password)}",
          "-e POSTGRES_DB=#{Shellwords.escape(@dbname)}",
          '--detach',
          '--rm',
          'postgres:18.1',
          @config.map { |k, v| "-c #{Shellwords.escape("#{k}=#{v}")}" }
        ],
        log: stdout
      )
    container = out.scan(/[a-f0-9]+\Z/).first
    File.write(File.join(home, 'docker-container'), container)
    at_exit do
      if qbash("docker ps --format '{{.ID}}' --no-trunc | grep '#{Shellwords.escape(container)}'",
               both: true, accept: nil)[1].zero?
        qbash("docker stop #{Shellwords.escape(container)}")
        puts "PostgreSQL docker container #{container.inspect} was stopped" unless @quiet
      end
    end
    begin
      WaitUtil.wait_for_service('PG in Docker', 'localhost', port, timeout_sec: 10, delay_sec: 0.1)
    rescue WaitUtil::TimeoutError => e
      raise "Failed to start PostgreSQL Docker container #{container.inspect}: #{e.message}"
    end
    container
  end

  def run_local(home, stdout, port)
    Tempfile.open do |pwfile|
      File.write(pwfile.path, @password)
      qbash(
        [
          'initdb',
          '--auth=trust',
          '-D',
          Shellwords.escape(home),
          '--username',
          Shellwords.escape(@user),
          '--pwfile',
          Shellwords.escape(pwfile.path)
        ],
        log: stdout
      )
    end
    cmd = [
      'postgres',
      '-k', Shellwords.escape(home),
      '-D', Shellwords.escape(home),
      @config.map { |k, v| "-c #{Shellwords.escape("#{k}=#{v}")}" },
      "--port=#{port}"
    ].join(' ')
    pid = Process.spawn(
      cmd,
      $stdout => File.join(home, 'stdout.txt'),
      $stderr => File.join(home, 'stderr.txt')
    )
    File.write(File.join(@dir, 'pid'), pid)
    at_exit do
      qbash("kill -TERM #{Shellwords.escape(pid)}", log: stdout)
      puts "PostgreSQL killed in PID #{pid}" unless @quiet
    end
    begin
      WaitUtil.wait_for_service('PG in local', 'localhost', port, timeout_sec: 10, delay_sec: 0.1)
    rescue WaitUtil::TimeoutError => e
      puts "+ #{cmd}"
      puts "stdout:\n#{File.read(File.join(home, 'stdout.txt'))}"
      puts "stderr:\n#{File.read(File.join(home, 'stderr.txt'))}"
      raise "Failed to start PostgreSQL database server on port #{port}: #{e.message}"
    end
    qbash(
      [
        'createdb',
        '--host', 'localhost',
        '--port', Shellwords.escape(port),
        '--username', Shellwords.escape(@user),
        Shellwords.escape(@dbname)
      ],
      log: stdout
    )
    pid
  end
end
