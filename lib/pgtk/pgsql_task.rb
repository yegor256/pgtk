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
# Copyright:: Copyright (c) 2019-2024 Yegor Bugayenko
# License:: MIT
class Pgtk::PgsqlTask < Rake::TaskLib
  attr_accessor :name, :dir, :fresh_start, :user, :password, :dbname, :yaml, :quiet, :port

  def initialize(*args, &task_block)
    super()
    @name = args.shift || :pgsql
    @fresh_start = false
    @quiet = false
    @user = 'test'
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
          'initdb --auth=trust',
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
    pid = Process.spawn(
      [
        'postgres',
        '-k', Shellwords.escape(home),
        '-D', Shellwords.escape(home),
        '-c', Shellwords.escape("log_directory=#{home}"),
        '-c', 'logging_collector=on',
        '-c', 'log_statement=all',
        '-c', 'log_filename=pgsql.log',
        "--port=#{port}"
      ].join(' '),
      $stdout => File.join(home, 'stdout.txt'),
      $stderr => File.join(home, 'stderr.txt')
    )
    File.write(File.join(@dir, 'pid'), pid)
    at_exit do
      `kill -TERM #{pid}`
      puts "PostgreSQL killed in PID #{pid}" unless @quiet
    end
    attempt = 0
    loop do
      TCPSocket.new 'localhost', port
      break
    rescue
      sleep(0.1)
      attempt += 1
      if attempt > 10
        puts File.read(File.join(home, 'stdout.txt'))
        puts File.read(File.join(home, 'stderr.txt'))
        raise "Failed to start PostgreSQL database server"
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
    puts "PostgreSQL has been started in process ##{pid}, port #{port}" unless @quiet
  end
end
