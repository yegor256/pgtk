<img src="https://upload.wikimedia.org/wikipedia/commons/2/29/Postgresql_elephant.svg" height="64px"/>

[![EO principles respected here](https://www.elegantobjects.org/badge.svg)](https://www.elegantobjects.org)
[![Managed by Zerocracy](https://www.0crat.com/badge/C3RFVLU72.svg)](https://www.0crat.com/p/C3RFVLU72)
[![DevOps By Rultor.com](http://www.rultor.com/b/yegor256/pgtk)](http://www.rultor.com/p/yegor256/pgtk)
[![We recommend RubyMine](https://www.elegantobjects.org/rubymine.svg)](https://www.jetbrains.com/ruby/)

[![Build Status](https://travis-ci.org/yegor256/pgtk.svg)](https://travis-ci.org/yegor256/pgtk)
[![Build status](https://ci.appveyor.com/api/projects/status/tbeaa0d4dk38xdb5?svg=true)](https://ci.appveyor.com/project/yegor256/pgtk)
[![PDD status](http://www.0pdd.com/svg?name=yegor256/pgtk)](http://www.0pdd.com/p?name=yegor256/pgtk)
[![Gem Version](https://badge.fury.io/rb/pgtk.svg)](http://badge.fury.io/rb/pgtk)
[![Maintainability](https://api.codeclimate.com/v1/badges/3a5bebac001e5288b00d/maintainability)](https://codeclimate.com/github/yegor256/pgtk/maintainability)

[![License](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/yegor256/pgtk/blob/master/LICENSE.txt)
[![Test Coverage](https://img.shields.io/codecov/c/github/yegor256/pgtk.svg)](https://codecov.io/github/yegor256/pgtk?branch=master)
[![Hits-of-Code](https://hitsofcode.com/github/yegor256/pgtk)](https://hitsofcode.com/view/github/yegor256/pgtk)

This small Ruby gem helps you integrate PostgreSQL with your Ruby
web app, through [Liquibase](https://www.liquibase.org/). It also adds a simple connection pool
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
$ sudo apt-get install -y postgresql-10 postgresql-client-10
$ sudo apt-get install -y default-jre maven
```

Then, add this to your [`Gemfile`](https://bundler.io/gemfile.html):

```ruby
gem 'pgtk'
```

Then, add this to your [`Rakefile`](https://github.com/ruby/rake/blob/master/doc/rakefile.rdoc):

```ruby
require 'pgtk/pgsql_task'
Pgtk::PgsqlTask.new :pgsql do |t|
  t.dir = 'target/pgsql' # Temp directory with PostgreSQL files
  t.fresh_start = true # To delete the directory on every start
  t.user = 'test'
  t.password = 'test'
  t.dbname = 'test'
  t.yaml = 'target/pgsql-config.yml' # YAML file to be created with connection details
end
```

And this too:

```ruby
require 'pgtk/liquibase_task'
Pgtk::LiquibaseTask.new liquibase: :pgsql do |t|
  t.master = 'liquibase/master.xml' # Master XML file path
  t.yaml = ['target/pgsql-config.yml', 'config.yml'] # YAML files with connection details
end
```

You should create that `liquibase/master.xml` file in your repository,
and a number of other XML files with Liquibase changes. This
[example](https://github.com/zold-io/wts.zold.io/tree/master/liquibase)
will help you understand them.

Now, you can do this:

```bash
$ bundle exec rake pgsql liquibase
```

A temporary PostgreSQL server will be started and the entire set of
Liquibase SQL changes will be applied. You will be able to connect
to it from your application, using the file `target/config.yml`.

From inside your app you may find this class useful:

```ruby
require 'pgtk/pool'
pgsql = Pgtk::Pool.new(Pgtk::Wire::Yaml.new('config.yml'))
pgsql.start(5) # Start it with five simultaneous connections
name = pgsql.exec('SELECT name FROM user WHERE id = $1', [id])[0]['name']
```

You may also use it if you need to run a transaction:

```ruby
pgsql.transaction do |t|
  t.exec('DELETE FROM user WHERE id = $1', [id])
  t.exec('INSERT INTO user (name, phone) VALUES ($1, $2)', [name, phone])
end
```

To make your PostgreSQL database visible in your unit test, I would
recommend you create a method `test_pgsql` in your `test__helper.rb` file
(which is `required` in all unit tests) and implement it like this:

```ruby
require 'yaml'
require 'minitest/autorun'
require 'pgtk/pool'
module Minitest
  class Test
    def test_pgsql
      config = YAML.load_file()
      @@test_pgsql ||= Pgtk::Pool.new(
        Pgtk::Wire::Yaml.new('target/pgsql-config.yml')
      ).start
    end
  end
end
```

Should work. Well, it works in [zold-io/wts.zold.io](https://github.com/zold-io/wts.zold.io)
and [yegor256/mailanes](https://github.com/yegor256/mailanes). They both are
open source, you can see how they use `pgtk`.

## How to contribute

Read [these guidelines](https://www.yegor256.com/2014/04/15/github-guidelines.html).
Make sure your build is green before you contribute
your pull request. You will need to have [Ruby](https://www.ruby-lang.org/en/) 2.3+ and
[Bundler](https://bundler.io/) installed. Then:

```
$ bundle update
$ bundle exec rake
```

If it's clean and you don't see any error messages, submit your pull request.

