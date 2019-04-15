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

  # Start it with a fixed number of connections. The amount of connections
  # is specified in +max+ argument and should be big enough to handle
  # the amount of parallel connections you may have to the database. However,
  # keep in mind that not all servers will allow you to have many connections
  # open at the same time. For example, Heroku free PostgreSQL database
  # allows only one connection open.
  def start(max = 8)
    max.times do
      @pool << PG.connect(
        dbname: @dbname, host: @host, port: @port,
        user: @user, password: @password
      )
    end
    self
  end

  # Make a query and return the result as an array of hashes. For example,
  # in order to fetch the list of all books belonging to the user:
  #
  #  books = pool.exec('SELECT * FROM book WHERE owner = $1', ['yegor256'])
  #  books.each do |row|
  #    puts 'ID: ' + row['id'].to_i
  #    puts 'Created: ' + Time.parse(row['created'])
  #    puts 'Title: ' + row['title']
  #  end
  #
  # All values in the retrieved hash are strings. No matter what types of
  # of data you have in the database, you get strings here. It's your job
  # to convert them to the type you need.
  #
  # In order to insert a new row (pay attention to the +RETURNING+ clause
  # at the end of the SQL query):
  #
  #  id = pool.exec(
  #    'INSERT INTO book (owner, title) VALUES ($1, $2) RETURNING id',
  #    ['yegor256', 'Elegant Objects']
  #  )[0]['id'].to_i
  #
  # You can also pass a block to this method, if you want to get an instance
  # of +PG::Result+ instead of an array of hashes:
  #
  #  pool.exec('SELECT * FROM book WHERE owner = $1', ['yegor256']) do |res|
  #    res.each do |row|
  #      puts 'ID: ' + row['id'].to_i
  #      puts 'Title: ' + row['title']
  #    end
  #  end
  #
  # More details about +exec_params+, which is called here, you can find
  # here: https://www.rubydoc.info/gems/pg/0.17.1/PG%2FConnection:exec_params
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
