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
  # A temporary class for logging.
  class Log
    def initialize(out)
      @out = out
    end

    def debug(msg)
      if @out.respond_to?(:debug)
        @out.debug(msg)
      elsif @out.respond_to?(:puts)
        @out.puts(msg)
      end
    end
  end

  # Constructor.
  def initialize(
    host: 'localhost', port:, dbname:, user:,
    password:, log: STDOUT
  )
    @host = host
    @port = port
    @dbname = dbname
    @user = user
    @password = password
    @pool = Queue.new
    @log = Log.new(log)
  end

  # Get the version of PostgreSQL server.
  def version
    @version ||= exec('SHOW server_version')[0]['server_version'].split(' ')[0]
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
    @log.debug("PostgreSQL pool started with #{max} connections")
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
  # When the query is too long it's convenient to use an array to specify it:
  #
  #  pool.exec(
  #    [
  #      'SELECT * FROM book',
  #      'LEFT JOIN user ON user.id = book.owner',
  #      'WHERE user.login = $1 AND book.title = $2'
  #    ],
  #    ['yegor256', 'Elegant Objects']
  #  )
  #
  # More details about +exec_params+, which is called here, you can find
  # here: https://www.rubydoc.info/gems/pg/0.17.1/PG%2FConnection:exec_params
  def exec(query, args = [], result = 0)
    connect do |c|
      t = Txn.new(c, @log)
      if block_given?
        t.exec(query, args, result) do |res|
          yield res
        end
      else
        t.exec(query, args, result)
      end
    end
  end

  # Run a transaction. The block has to be provided. It will receive
  # a temporary object, which implements method +exec+, which works
  # exactly like the method +exec+ of class +Pool+, for example:
  #
  #  pgsql.transaction do |t|
  #    t.exec('DELETE FROM user WHERE id = $1', [id])
  #    t.exec('INSERT INTO user (name) VALUES ($1)', [name])
  #  end
  def transaction
    connect do |c|
      t = Txn.new(c, @log)
      yield t
    end
  end

  # A temporary class to execute a single SQL request.
  class Txn
    def initialize(conn, log)
      @conn = conn
      @log = log
    end

    def exec(query, args = [], result = 0)
      start = Time.now
      sql = query.is_a?(Array) ? query.join(' ') : query
      out = @conn.exec_params(sql, args, result) do |res|
        if block_given?
          yield res
        else
          rows = []
          res.each { |r| rows << r }
          rows
        end
      end
      @log.debug("#{sql}: #{(start - Time.now).round}ms")
      out
    end
  end

  private

  def connect
    conn = @pool.pop
    begin
      yield conn
    ensure
      @pool << conn
    end
  end
end
