# Ruby + PostgreSQL + Liquibase + Rake

[![EO principles respected here](https://www.elegantobjects.org/badge.svg)](https://www.elegantobjects.org)
[![DevOps By Rultor.com](https://www.rultor.com/b/yegor256/pgtk)](https://www.rultor.com/p/yegor256/pgtk)
[![We recommend RubyMine](https://www.elegantobjects.org/rubymine.svg)](https://www.jetbrains.com/ruby/)

[![rake](https://github.com/yegor256/pgtk/actions/workflows/rake.yml/badge.svg)](https://github.com/yegor256/pgtk/actions/workflows/rake.yml)
[![PDD status](https://www.0pdd.com/svg?name=yegor256/pgtk)](https://www.0pdd.com/p?name=yegor256/pgtk)
[![Gem Version](https://badge.fury.io/rb/pgtk.svg)](https://badge.fury.io/rb/pgtk)
[![Maintainability](https://api.codeclimate.com/v1/badges/3a5bebac001e5288b00d/maintainability)](https://codeclimate.com/github/yegor256/pgtk/maintainability)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/yegor256/pgtk/blob/master/LICENSE.txt)
[![Test Coverage](https://img.shields.io/codecov/c/github/yegor256/pgtk.svg)](https://codecov.io/github/yegor256/pgtk?branch=master)
[![Hits-of-Code](https://hitsofcode.com/github/yegor256/pgtk)](https://hitsofcode.com/view/github/yegor256/pgtk)

This small Ruby gem helps you integrate
[PostgreSQL](https://www.postgresql.org/) with your Ruby
web app, through [Liquibase](https://www.liquibase.org/).
It also adds a simple connection pool
and query processor, to make SQL manipulation simpler.

First of all, on top of
[Ruby](https://www.ruby-lang.org/en/) and
[Bundler](https://bundler.io/)
you need to have
[PostgreSQL](https://www.postgresql.org/),
[Java 8+](https://java.com/en/download/), and
[Maven 3.2+](https://maven.apache.org/) installed.
In Ubuntu 16+ this should be enough:

```bash
sudo apt-get install -y postgresql-10 postgresql-client-10
sudo apt-get install -y default-jre maven
```

Then, add this to your [Gemfile](https://bundler.io/gemfile.html):

```ruby
gem 'pgtk'
```

Then, add this to your
[Rakefile](https://github.com/ruby/rake/blob/master/doc/rakefile.rdoc):

```ruby
require 'pgtk/pgsql_task'
Pgtk::PgsqlTask.new :pgsql do |t|
  # Temp directory with PostgreSQL files:
  t.dir = 'target/pgsql'
  # To delete the directory on every start;
  t.fresh_start = true
  t.user = 'test'
  t.password = 'test'
  t.dbname = 'test'
  # YAML file to be created with connection details:
  t.yaml = 'target/pgsql-config.yml'
  # List of contexts or empty if all:
  t.contexts = '!test'
  # List of PostgreSQL configuration options:
  t.config = {
    log_min_messages: 'ERROR',
    log_filename: 'target/pg.log'
  }
end
```

And this too
([org.postgresql:postgresql][plugin-1]
and [org.liquibase:liquibase-maven-plugin][plugin-2]
are used inside):

```ruby
require 'pgtk/liquibase_task'
Pgtk::LiquibaseTask.new liquibase: :pgsql do |t|
  # Master XML file path:
  t.master = 'liquibase/master.xml'
  # YAML files connection details:
  t.yaml = ['target/pgsql-config.yml', 'config.yml']
  # Reduce the amount of log messages (TRUE by default):
  t.quiet = false
  # Overwriting default version of PostgreSQL server:
  t.postgresql_version = '42.7.0'
  # Overwriting default version of Liquibase:
  t.liquibase_version = '3.2.2'
end
```

The `config.yml` file should be in this format:

```yaml
pgsql:
  url: jdbc:postgresql://<host>:<port>/<dbname>?user=<user>
  host: ...
  port: ...
  dbname: ...
  user: ...
  password: ...
```

You should create that `liquibase/master.xml` file in your repository,
and a number of other XML files with Liquibase changes. This
[example](https://github.com/zold-io/wts.zold.io/tree/master/liquibase)
will help you understand them.

Now, you can do this:

```bash
bundle exec rake pgsql liquibase
```

A temporary PostgreSQL server will be started and the entire set of
Liquibase SQL changes will be applied. You will be able to connect
to it from your application, using the file `target/pgsql-config.yml`.

From inside your app you may find this class useful:

```ruby
require 'pgtk/pool'
pgsql = Pgtk::Pool.new(Pgtk::Wire::Yaml.new('config.yml'))
pgsql.start(5) # Start it with five simultaneous connections
```

You can also let it pick the connection parameters from the environment
variable `DATABASE_URL`, formatted like
`postgres://user:password@host:5432/dbname`:

```ruby
pgsql = Pgtk::Pool.new(Pgtk::Wire::Env.new)
```

Now you can fetch some data from the DB:

```ruby
name = pgsql.exec('SELECT name FROM user WHERE id = $1', [id])[0]['name']
```

You may also use it when you need to run a transaction:

```ruby
pgsql.transaction do |t|
  t.exec('DELETE FROM user WHERE id = $1', [id])
  t.exec('INSERT INTO user (name, phone) VALUES ($1, $2)', [name, phone])
end
```

To make your PostgreSQL database visible in your unit tests, I would
recommend you create a method `test_pgsql` in your `test__helper.rb` file
(which is `required` in all unit tests) and implement it like this:

```ruby
require 'yaml'
require 'minitest/autorun'
require 'pgtk/pool'
module Minitest
  class Test
    def test_pgsql
      @@test_pgsql ||= Pgtk::Pool.new(
        Pgtk::Wire::Yaml.new('target/pgsql-config.yml')
      ).start
    end
  end
end
```

## Logging with `Pgtk::Spy`

You can also track all SQL queries sent through the pool,
with the help of `Pgtk::Spy`:

```ruby
require 'pgtk/spy'
pool = Pgtk::Spy.new(pool) do |sql|
  # here, save this "sql" somewhere
end
```

## Query Timeouts with `Pgtk::Impatient`

To prevent queries from running indefinitely, use `Pgtk::Impatient` to enforce
timeouts on database operations:

```ruby
require 'pgtk/impatient'
# Wrap the pool with a 5-second timeout for all queries
impatient = Pgtk::Impatient.new(pool, 5)
```

The impatient decorator ensures queries don't hang your application:

```ruby
begin
  # This query will be terminated if it takes longer than 5 seconds
  impatient.exec('SELECT * FROM large_table WHERE complex_condition')
rescue Pgtk::Impatient::TooSlow => e
  puts "Query timed out: #{e.message}"
end
```

You can exclude specific queries from timeout enforcement using regex patterns:

```ruby
# Don't timeout any SELECT queries or specific maintenance operations
impatient = Pgtk::Impatient.new(pool, 2, /^SELECT/, /^VACUUM/)
```

Key features:

1. Configurable timeout in seconds for each query
2. Raises `Pgtk::Impatient::TooSlow` exception when timeout is exceeded
3. Can exclude queries matching specific patterns from timeout checks
4. Also sets PostgreSQL's `statement_timeout` for transactions

## Query Caching with `Pgtk::Stash`

For applications with frequent read queries,
you can use `Pgtk::Stash` to add a caching layer:

```ruby
require 'pgtk/stash'
stash = Pgtk::Stash.new(pgsql)
```

`Stash` automatically caches read queries and invalidates the cache
when tables are modified:

```ruby
# First execution runs the query against the database
result1 = stash.exec('SELECT * FROM users WHERE id = $1', [123])
# Second execution with the same query and parameters returns cached result
result2 = stash.exec('SELECT * FROM users WHERE id = $1', [123])
# This modifies the 'users' table, invalidating any cached queries for that table
stash.exec('UPDATE users SET name = $1 WHERE id = $2', ['John', 123])
# This will execute against the database again since cache was invalidated
result3 = stash.exec('SELECT * FROM users WHERE id = $1', [123])
```

Note that the caching implementation is basic and only suitable
for simple queries:

1. Queries must reference tables (using `FROM` or `JOIN`)
2. Cache is invalidated by table, not by specific rows
3. Write operations (`INSERT`, `UPDATE`, `DELETE`) bypass
the cache and invalidate all cached queries for affected tables

## Automatic Retries with `Pgtk::Retry`

For resilient database operations, `Pgtk::Retry` provides automatic retry
functionality for failed `SELECT` queries:

```ruby
require 'pgtk/retry'
# Wrap the pool with retry functionality (default: 3 attempts)
retry_pool = Pgtk::Retry.new(pgsql)
# Or specify custom number of attempts
retry_pool = Pgtk::Retry.new(pgsql, attempts: 5)
```

The retry decorator automatically retries `SELECT` queries that fail due to
transient errors (network issues, connection problems, etc.):

```ruby
# This SELECT will be retried up to 3 times if it fails
users = retry_pool.exec('SELECT * FROM users WHERE active = true')

# Non-SELECT queries are NOT retried to prevent duplicate writes
retry_pool.exec('INSERT INTO logs (message) VALUES ($1)', ['User logged in'])
```

Key features:

1. Only `SELECT` queries are retried (to prevent duplicate data modifications)
2. Retries happen immediately without delay
3. The original error is raised after all retry attempts are exhausted
4. Works seamlessly with other decorators like `Pgtk::Spy` and `Pgtk::Impatient`

## Some Examples

This library works in
[netbout.com](https://github.com/yegor256/netbout),
[wts.zold.io](https://github.com/zold-io/wts.zold.io),
[mailanes.com](https://github.com/yegor256/mailanes), and
[0rsk.com](https://github.com/yegor256/0rsk).

They are all open source, you can see how they use `pgtk`.

## How to contribute

Read
[these guidelines](https://www.yegor256.com/2014/04/15/github-guidelines.html).
Make sure your build is green before you contribute
your pull request. You will need to have
[Ruby](https://www.ruby-lang.org/en/) 2.3+ and
[Bundler](https://bundler.io/) installed. Then:

```bash
bundle update
bundle exec rake
```

If it's clean and you don't see any error messages, submit your pull request.

To run a single test, do this:

```bash
bundle exec ruby test/test_pool.rb -n test_basic
```

[plugin-1]: https://mvnrepository.com/artifact/org.postgresql/postgresql
[plugin-2]: https://mvnrepository.com/artifact/org.liquibase/liquibase-maven-plugin
