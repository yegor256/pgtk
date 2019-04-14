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

require 'pg'
require_relative '../pgtk'

# Pool.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019 Yegor Bugayenko
# License:: MIT
class Pgtk::Pool
  # Constructor.
  def initialize(host: 'localhost', port:, dbname:, user:, password:)
    @host = host
    @port = port
    @port = port
    @dbname = dbname
    @user = user
    @password = password
    @pool = Queue.new
  end

  # Start it with a fixed number of connections.
  def start(max = 1)
    max.times do
      @pool << PG.connect(
        dbname: @dbname, host: @host, port: @port,
        user: @user, password: @password
      )
    end
    self
  end

  # Make a query and return the result as an array of hashes.
  def exec(query, args = [], result = 0)
    connect do |c|
      c.exec_params(query, args, result) do |res|
        if block_given?
          yield res
        else
          rows = []
          res.each { |r| rows << r }
          rows
        end
      end
    end
  end

  # Get a connection from the pool and let us work with it. The block
  # has to be provided, for example:
  #
  #  pgsql.connect do |c|
  #    c.transaction do |conn|
  #      conn.exec_params('DELETE FROM user WHERE id = $1', [id])
  #      conn.exec_params('INSERT INTO user (name) VALUES ($1)', [name])
  #    end
  #  end
  def connect
    conn = @pool.pop
    begin
      yield conn
    ensure
      @pool << conn
    end
  end
end
