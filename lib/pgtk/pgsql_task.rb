# frozen_string_literal: true

require 'English'
# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'cgi'
require 'qbash'
require 'rake'
require 'rake/tasklib'
require 'random-port'
require 'securerandom'
require 'shellwords'
require 'tempfile'
require 'waitutil'
require 'yaml'
require_relative '../pgtk'

# Pgsql rake task.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class Pgtk::PgsqlTask < Rake::TaskLib
  attr_accessor :name, :dir, :fresh, :user, :password, :dbname, :yaml, :quiet, :port, :config, :docker

  # Initialize a new PostgreSQL server task.
  #
  # @param [Array] args Task arguments
  # @yield [Pgtk::PgsqlTask, Object] Yields self and task arguments
  def initialize(*args, &task_block)
    super()
    @docker ||= :maybe
    @name = args.shift || :pgsql
    @fresh = false
    @quiet = false
    @user = 'test'
    @config = {}
    @password = 'test'
    @dbname = 'test'
    @port = nil
    desc('Start a local PostgreSQL server') unless ::Rake.application.last_description
    task(name, *args) do |_, task_args|
      RakeFileUtils.verbose(true) do
        yield(*[self, task_args].slice(0, task_block.arity)) if block_given?
        run
      end
    end
  end

  private

  def run
    local = detect(:local)
    docker = detect(:docker)
    preflight(local, docker)
    home = File.expand_path(@dir)
    FileUtils.rm_rf(home) if @fresh
    raise(ArgumentError, "Directory/file #{home} is present, use fresh=true") if File.exist?(home)
    stdout = @quiet ? nil : $stdout
    port = acquire
    place = launch(local, home, stdout, port)
    save(port)
    return if @quiet
    puts("PostgreSQL has been started #{place}, port #{port}")
    puts("YAML config saved to #{@yaml}")
  end

  def preflight(local, docker)
    raise(IOError, 'Failed to find either PostgreSQL or Docker') unless local || docker
    if @docker == :always && !docker
      raise(ArgumentError, 'You cannot force Docker to run, because it is not installed locally')
    end
    raise(ArgumentError, "Option 'dir' is mandatory") unless @dir
    raise(ArgumentError, "Option 'yaml' is mandatory") unless @yaml
  end

  def acquire
    port = @port
    if port.nil?
      port = RandomPort::Pool::SINGLETON.acquire
      puts("Random TCP port #{port} is used for PostgreSQL server") unless @quiet
    else
      puts("Required TCP port #{port} is used for PostgreSQL server") unless @quiet
    end
    port
  end

  def launch(local, home, stdout, port)
    if (local && @docker != :always) || @docker == :never
      pid = localize(home, stdout, port)
      "in process ##{pid}"
    else
      container = dockerize(home, stdout, port)
      "in container #{container}"
    end
  end

  def save(port)
    File.write(
      @yaml,
      {
        'pgsql' => {
          'host' => 'localhost',
          'port' => port,
          'dbname' => @dbname,
          'user' => @user,
          'password' => @password,
          'url' => ["jdbc:postgresql://localhost:#{port}/", "#{CGI.escape(@dbname)}?user=#{CGI.escape(@user)}"].join
        }
      }.to_yaml
    )
  end

  def detect(what)
    case what
    when :local
      qbash('postgres -V; initdb -V', accept: nil, both: true)[1].zero?
    when :docker
      qbash('docker -v', accept: nil, both: true)[1].zero?
    end
  end

  def dockerize(home, stdout, port)
    FileUtils.mkdir_p(home)
    out =
      qbash(
        'docker',
        'run',
        "--publish #{Shellwords.escape("#{port}:5432")}",
        "-e POSTGRES_USER=#{Shellwords.escape(@user)}",
        "-e POSTGRES_PASSWORD=#{Shellwords.escape(@password)}",
        "-e POSTGRES_DB=#{Shellwords.escape(@dbname)}",
        '--detach',
        '--rm',
        'postgres:18.1',
        @config.map { |k, v| "-c #{Shellwords.escape("#{k}=#{v}")}" },
        stdout:
      )
    container = out.scan(/[a-f0-9]+\Z/).first
    File.write(File.join(home, 'docker-container'), container)
    at_exit do
      if qbash(
        "docker ps --format '{{.ID}}' --no-trunc | grep '#{Shellwords.escape(container)}'",
        both: true, accept: nil
      )[1].zero?
        qbash("docker stop #{Shellwords.escape(container)}")
        puts("PostgreSQL docker container #{container.inspect} was stopped") unless @quiet
      end
    end
    begin
      WaitUtil.wait_for_service('PG in Docker', 'localhost', port, timeout_sec: 10, delay_sec: 0.1)
    rescue WaitUtil::TimeoutError => e
      raise(IOError, "Failed to start PostgreSQL Docker container #{container.inspect}: #{e.message}")
    end
    container
  end

  def localize(home, stdout, port)
    Tempfile.open do |pwfile|
      File.write(pwfile.path, @password)
      qbash(
        'initdb',
        '--auth=trust',
        '--locale=en_US.UTF-8',
        '-D',
        Shellwords.escape(home),
        '--username',
        Shellwords.escape(@user),
        '--pwfile',
        Shellwords.escape(pwfile.path),
        stdout:
      )
    end
    cmd = [
      'postgres',
      '-k', Shellwords.escape(home),
      '-D', Shellwords.escape(home),
      @config.map { |k, v| "-c #{Shellwords.escape("#{k}=#{v}")}" },
      "--port=#{port}"
    ].join(' ')
    pid = Process.spawn(cmd, $stdout => File.join(home, 'stdout.txt'), $stderr => File.join(home, 'stderr.txt'))
    File.write(File.join(@dir, 'pid'), pid)
    at_exit do
      qbash("kill -TERM #{Shellwords.escape(pid)}", stdout:)
      puts("PostgreSQL killed in PID #{pid}") unless @quiet
    end
    begin
      WaitUtil.wait_for_service('PG in local', 'localhost', port, timeout_sec: 10, delay_sec: 0.1)
    rescue WaitUtil::TimeoutError => e
      puts("+ #{cmd}")
      puts("stdout:\n#{File.read(File.join(home, 'stdout.txt'))}")
      puts("stderr:\n#{File.read(File.join(home, 'stderr.txt'))}")
      raise(IOError, "Failed to start PostgreSQL database server on port #{port}: #{e.message}")
    end
    qbash(
      'createdb',
      '--host', 'localhost',
      '--port', Shellwords.escape(port),
      '--username', Shellwords.escape(@user),
      Shellwords.escape(@dbname),
      stdout:
    )
    pid
  end
end
