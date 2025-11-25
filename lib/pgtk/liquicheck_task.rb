# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'nokogiri'
require 'rake/tasklib'
require_relative '../pgtk'

# Liquicheck rake task for check Liquibase XML files.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2025 Yegor Bugayenko
# License:: MIT
class Pgtk::LiquicheckTask < Rake::TaskLib
  # Task name
  # @return [Symbol]
  attr_accessor :name

  # Base directory where Liquibase XML files will be stored
  # @return [String]
  attr_accessor :dir

  # Migration XML files pattern
  # @return [String]
  attr_accessor :pattern

  def initialize(*args, &task_block)
    super()
    @name = args.shift || :liquicheck
    @dir = 'liquibase'
    @pattern = '*/*.xml'
    desc 'Check the quality of Liquibase XML files' unless ::Rake.application.last_description
    task(name, *args) do |_, task_args|
      RakeFileUtils.send(:verbose, true) do
        yield(*[self, task_args].slice(0, task_block.arity)) if block_given?
        run
      end
    end
  end

  private

  def run
    raise "Option 'dir' is mandatory" if !@dir || @dir.empty?
    raise "Option 'pattern' is mandatory" if !@pattern || @pattern.empty?
    errors = {}
    Dir[File.join(File.expand_path(File.join(Dir.pwd, @dir)), @pattern)].each do |f|
      err = FileErrors.new(f, errors)
      doc = Nokogiri::XML(File.open(f))
      doc.remove_namespaces!
      path = decent(doc.at_xpath('databaseChangeLog')&.attr('logicalFilePath')&.to_s, 'logicalFilePath', err)
      doc.xpath('databaseChangeLog/changeSet').each do |node|
        id = decent(node.attr('id')&.to_s, 'ID', err)
        author = decent(node.attr('author')&.to_s, 'author', err)
        context = node.attr('context')&.to_s
        present(author) do
          no_match(author, /\A[-_ A-Za-z0-9]+\z/) do
            err.add("author #{author.inspect} has illegal symbols#{' in test context' if context == 'test'}")
          end
        end
        if context == 'test'
          present(id) do
            no_match(id, /[-a-z]+/) do
              err.add("ID #{id.inspect} has not suffix in test context")
            end
          end
          present(id, path) do
            match(id, /[-a-z]+/) do
              no_match(path.gsub(/[-_.a-z]/, ''), /\A#{id.gsub(/[-a-z]/, '')}\z/) do
                err.add("ID #{id.inspect} is not the beginning of a logicalFilePath #{path.inspect} in test context")
              end
            end
          end
        else
          present(id, path) do
            no_match(path.gsub(/[-_.a-z]/, ''), /\A#{id}\z/) do
              err.add("ID #{id.inspect} is not the beginning of a logicalFilePath #{path.inspect}")
            end
          end
        end
      end
      present(path) do
        no_equal(path, File.basename(f)) do
          err.add("logicalFilePath #{path.inspect} does not match the xml file name #{File.basename(f).inspect}")
        end
      end
    end
    return if errors.empty?
    puts 'There are such errors in the Liquibase XML files.'
    errors.each do |f, e|
      puts "In file '#{f}':"
      e.each do |msg|
        puts "  * #{msg}"
      end
      puts
    end
    exit(1)
  end

  def decent(prop, name, err)
    if prop.nil?
      err.add("#{name} is nil")
    elsif prop.empty?
      err.add("#{name} is empty")
    end
    prop
  end

  def present(*props, &)
    yield if props.all? { !_1.nil? && !_1.empty? }
  end

  def no_equal(prop, val, &)
    yield if prop != val
  end

  def match(prop, regex, &)
    yield if prop.match?(regex)
  end

  def no_match(prop, regex, &)
    yield unless prop.match?(regex)
  end

  # Errors that were found in the specified file
  class FileErrors
    def initialize(filepath, errors)
      @filepath = filepath
      @errors = errors
    end

    def add(msg)
      (@errors[@filepath] ||= []) << msg
    end
  end
end
