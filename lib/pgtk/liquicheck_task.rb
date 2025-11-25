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
    Dir[File.join(File.expand_path(File.join(Dir.pwd, @dir)), @pattern)].each do |file|
      doc = Nokogiri::XML(File.open(file))
      doc.remove_namespaces!
      path = doc.at_xpath('databaseChangeLog')&.attr('logicalFilePath')&.to_s
      on(errors, file) do
        must_have(path, 'logicalFilePath is empty')
        must_equal(
          path,
          File.basename(file),
          "logicalFilePath #{path.inspect} does not equal the xml file name #{File.basename(file).inspect}"
        )
      end
      doc.xpath('databaseChangeLog/changeSet').each do |node|
        id = node.attr('id')&.to_s
        author = node.attr('author')&.to_s
        context = node.attr('context')&.to_s
        on(errors, file) do
          must_have(id, 'ID is empty')
          must_match(id, /[-a-z]+/, "ID #{id.inspect} has not suffix in #{context} context") if context
        end
        on(errors, file) do
          must_have(author, 'author is empty')
          must_match(
            author,
            /\A[-_ A-Za-z0-9]+\z/,
            "author #{author.inspect} has illegal symbols"
          )
        end
        on(errors, file) do
          must_have(id, 'ID is empty')
          must_have(path, 'logicalFilePath is empty')
          must_match(
            path.gsub(/[-_.a-z]/, ''),
            /\A#{id.gsub(/[-a-z]/, '')}\z/,
            "ID #{id.inspect} is not the beginning of a logicalFilePath #{path.inspect}"
          )
        end
      end
    end
    return if errors.empty?
    puts 'There are such errors in the Liquibase XML files.'
    errors.each do |f, e|
      puts "In file '#{f}':"
      e.uniq.each do |msg|
        puts "  * #{msg}"
      end
      puts
    end
    exit(1)
  end

  def on(errors, file, &)
    yield if block_given?
  rescue MustError => e
    (errors[file] ||= []) << e.message
  end

  def must_have(prop, msg)
    (raise MustError, msg) if prop.nil? || prop.empty?
  end

  def must_equal(lprop, rprop, msg)
    (raise MustError, msg) if lprop != rprop
  end

  def must_match(prop, regex, msg)
    (raise MustError, msg) unless prop.match?(regex)
  end

  MustError = Class.new(StandardError)
  private_constant :MustError
end
