# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'cgi'
require 'English'
require 'qbash'
require 'rake'
require 'rake/tasklib'
require 'random-port'
require 'shellwords'
require 'tempfile'
require 'yaml'
require_relative '../pgtk'

# Pgsql rake task.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2025 Yegor Bugayenko
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
    raise "Option 'dir' is mandatory" unless @dir
    raise "Option 'yaml' is mandatory" unless @yaml
    home = File.expand_path(@dir)
    FileUtils.rm_rf(home) if @fresh_start
    raise "Directory/file #{home} is present, use fresh_start=true" if File.exist?(home)
    stdout = @quiet ? nil : $stdout
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
    port = @port
    if port.nil?
      port = RandomPort::Pool::SINGLETON.acquire
      puts "Random TCP port #{port} is used for PostgreSQL server" unless @quiet
    else
      puts "Required TCP port #{port} is used for PostgreSQL server" unless @quiet
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
      qbash("kill -TERM #{pid}", log: stdout)
      puts "PostgreSQL killed in PID #{pid}" unless @quiet
    end
    attempt = 0
    begin
      TCPSocket.new('localhost', port)
    rescue StandardError => e
      sleep(0.1)
      attempt += 1
      if attempt > 50
        puts "+ #{cmd}"
        puts "stdout:\n#{File.read(File.join(home, 'stdout.txt'))}"
        puts "stderr:\n#{File.read(File.join(home, 'stderr.txt'))}"
        raise "Failed to start PostgreSQL database server on port #{port}: #{e.message}"
      end
      retry
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
    puts "PostgreSQL has been started in process ##{pid}, port #{port}"
    puts "YAML config saved to #{@yaml}"
  end
end
