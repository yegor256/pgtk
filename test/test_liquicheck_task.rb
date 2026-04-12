# frozen_string_literal: true

require 'nokogiri'
require 'qbash'
# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'tmpdir'
require_relative '../lib/pgtk/liquicheck_task'
require_relative 'test__helper'

# Liquicheck rake task test.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2017-2026 Yegor Bugayenko
# License:: MIT
class TestLiquicheckTask < Pgtk::Test
  def test_simple
    Dir.mktmpdir do |dir|
      populate(dir, simple)
      Pgtk::LiquicheckTask.new(:liquicheck) do |t|
        t.dir = '.'
        t.pattern = '*.xml'
      end
      assert_raises(SystemExit) do
        assert_output(/There are such errors in the Liquibase XML files/) do
          Dir.stub(:pwd, dir) do
            Rake::Task['liquicheck'].invoke
          end
        end
      end
    end
  end

  def test_without_xml
    Dir.mktmpdir do |dir|
      File.join(File.expand_path(__dir__), '..', 'lib', 'pgtk', 'liquicheck_task').then do |r|
        File.write(File.join(dir, 'Rakefile'), <<~RUBY)
          require '#{r}'
          Pgtk::LiquicheckTask.new
        RUBY
      end
      assert_empty(qbash("cd #{Shellwords.escape(dir)} && bundle exec rake liquicheck"))
    end
  end

  def test_without_errors
    Dir.mktmpdir do |dir|
      File.write(File.join(dir, 'Rakefile'), rakefile)
      populate(dir, clean)
      assert_empty(qbash("cd #{Shellwords.escape(dir)} && bundle exec rake liquicheck"))
    end
  end

  def test_with_empty_attributes
    Dir.mktmpdir do |dir|
      File.write(File.join(dir, 'Rakefile'), rakefile)
      populate(dir, empties)
      out, e = qbash("cd #{Shellwords.escape(dir)} && bundle exec rake liquicheck", accept: nil, both: true)
      refute_empty(out)
      assert_equal(1, e)
      assert_match(/001-some-migration.xml.+\n.+logicalFilePath is empty/, out)
      assert_match(/002-some-migration.xml.+\n.+ID is empty/, out)
      assert_match(/003-some-migration.xml.+\n.+author is empty/, out)
      assert_match(/004-some-migration.xml.+\n.+logicalFilePath is empty/, out)
      assert_match(/005-some-migration.xml.+\n.+ID is empty/, out)
      assert_match(/006-some-migration.xml.+\n.+author is empty/, out)
    end
  end

  def test_with_incorrect_attributes
    Dir.mktmpdir do |dir|
      File.write(File.join(dir, 'Rakefile'), rakefile)
      populate(dir, incorrect)
      out, e = qbash("cd #{Shellwords.escape(dir)} && bundle exec rake liquicheck", accept: nil, both: true)
      refute_empty(out)
      assert_equal(1, e)
      assert_match(/001-some-migration.xml.+\n.+author "!\*" has illegal symbols/, out)
      assert_match(/002-some-migration.xml.+\n.+author "\\#@" has illegal symbols/, out)
      assert_match(
        /003-some-migration.xml.+\n.+ID "00" is not the beginning of a logicalFilePath "003-some-migration.xml"/,
        out
      )
      assert_match(
        /004-some-migration.xml.+\n.+ID "00-test" is not the beginning of a logicalFilePath "004-some-migration.xml"/,
        out
      )
      assert_match(/005-some-migration.xml.+\n.+ID "005" has not suffix/, out)
      assert_match(
        /006-some-migration.xml.+\n.+logicalFilePath\s
        "006-other-migration.xml"\sdoes\snot\sequal\sthe\sxml\sfile\sname\s"006-some-migration.xml"/x,
        out
      )
    end
  end

  private

  def populate(dir, specs)
    specs.each do |name, opts|
      File.write(File.join(dir, name), migration(**opts))
    end
  end

  def simple
    {
      '001-some-migration.xml' => { logical_file_path: '001-some-migration.xml', id: '001', author: 'yegor256' },
      '002-some-migration.xml' => { logical_file_path: '002-some-migration.xml', id: '', author: 'user' },
      '003-migration.xml' => { logical_file_path: '003-some-migration.xml', id: '123', author: '*' },
      '004-some-migration.xml' => { logical_file_path: '004-migration.xml', id: '400', author: '*', context: 'test' },
      '005-some-migration.xml' => {
        logical_file_path: '005-migration.xml', id: '006-test', author: 'user', context: 'test'
      }
    }
  end

  def clean
    {
      '001-some-migration.xml' => { logical_file_path: '001-some-migration.xml', id: '001', author: 'yegor256' },
      '002-other-migration.xml' => { logical_file_path: '002-other-migration.xml', id: '002', author: 'yegor256' },
      '003-some-migration.xml' => {
        logical_file_path: '003-some-migration.xml', id: '003-test', author: 'yegor256', context: 'test'
      }
    }
  end

  def empties
    {
      '001-some-migration.xml' => { logical_file_path: '', id: '001', author: 'user' },
      '002-some-migration.xml' => { logical_file_path: '002-some-migration.xml', id: '', author: 'user' },
      '003-some-migration.xml' => { logical_file_path: '003-some-migration.xml', id: '003', author: '' },
      '004-some-migration.xml' => { logical_file_path: '', id: '004-test', author: 'user', context: 'test' },
      '005-some-migration.xml' => {
        logical_file_path: '005-some-migration.xml', id: '', author: 'user', context: 'test'
      },
      '006-some-migration.xml' => {
        logical_file_path: '006-some-migration.xml', id: '006-test', author: '', context: 'test'
      }
    }
  end

  def incorrect
    {
      '001-some-migration.xml' => { logical_file_path: '001-some-migration.xml', id: '001', author: '!*' },
      '002-some-migration.xml' => {
        logical_file_path: '002-some-migration.xml', id: '002-test', author: '#@', context: 'test'
      },
      '003-some-migration.xml' => { logical_file_path: '003-some-migration.xml', id: '00', author: 'user' },
      '004-some-migration.xml' => { logical_file_path: '004-some-migration.xml', id: '00-test', author: 'user' },
      '005-some-migration.xml' => {
        logical_file_path: '005-some-migration.xml', id: '005', author: 'user', context: 'test'
      },
      '006-some-migration.xml' => { logical_file_path: '006-other-migration.xml', id: '006', author: 'user' }
    }
  end

  def rakefile
    File.join(File.expand_path(__dir__), '..', 'lib', 'pgtk', 'liquicheck_task').then do |r|
      <<~RUBY
        require '#{r}'
        Pgtk::LiquicheckTask.new do |t|
          t.dir = '.'
          t.pattern = '*.xml'
        end
      RUBY
    end
  end

  def migration(logical_file_path: '', id: '', author: '', context: nil, sql: 'CREATE TABLE t (f TEXT);')
    Nokogiri::XML::Builder.new(encoding: 'UTF-8') do |xml|
      xml.comment(<<~TXT)
        * SPDX-FileCopyrightText: Copyright (c) 2025 Yegor Bugayenko
        * SPDX-License-Identifier: MIT
      TXT
      xml.databaseChangeLog(
        'xmlns' => 'http://www.liquibase.org/xml/ns/dbchangelog',
        'xmlns:xsi' => 'http://www.w3.org/2001/XMLSchema-instance',
        'xsi:schemaLocation' => 'http://www.liquibase.org/xml/ns/dbchangelog' \
                                'http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-2.0.xsd',
        'logicalFilePath' => logical_file_path
      ) do
        xml.changeSet({ 'id' => id, 'author' => author, 'context' => context }.compact) do
          xml.sql do
            xml.text(sql)
          end
        end
      end
    end.to_xml
  end
end
