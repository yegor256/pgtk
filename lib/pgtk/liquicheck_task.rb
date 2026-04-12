# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2026 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'nokogiri'
require 'rake/tasklib'
require_relative '../pgtk'

# Liquicheck rake task for check Liquibase XML files.
# Author:: Yegor Bugayenko (yegor256@gmail.com)
# Copyright:: Copyright (c) 2019-2026 Yegor Bugayenko
# License:: MIT
class Pgtk::LiquicheckTask < Rake::TaskLib
  attr_accessor :name, :dir, :pattern

  def initialize(*args, &task_block)
    super()
    @name = args.shift || :liquicheck
    @dir = 'liquibase'
    @pattern = '*/*.xml'
    desc('Check the quality of Liquibase XML files') unless ::Rake.application.last_description
    task(name, *args) do |_, task_args|
      RakeFileUtils.verbose(true) do
        yield(*[self, task_args].slice(0, task_block.arity)) if block_given?
        run
      end
    end
  end

  private

  def run
    raise(ArgumentError, "Option 'dir' is mandatory") if !@dir || @dir.empty?
    raise(ArgumentError, "Option 'pattern' is mandatory") if !@pattern || @pattern.empty?
    errors = {}
    Dir[File.join(File.expand_path(File.join(Dir.pwd, @dir)), @pattern)].each do |file|
      inspect(errors, file)
    end
    report(errors)
  end

  def inspect(errors, file)
    doc = Nokogiri::XML(File.open(file))
    doc.remove_namespaces!
    path = doc.at_xpath('databaseChangeLog')&.attr('logicalFilePath')&.to_s
    on(errors, file) do
      demand(path, 'logicalFilePath is empty')
      equate(
        path,
        File.basename(file),
        "logicalFilePath #{path.inspect} does not equal the xml file name #{File.basename(file).inspect}"
      )
    end
    doc.xpath('databaseChangeLog/changeSet').each do |node|
      verify(errors, file, node, path)
    end
  end

  def verify(errors, file, node, path)
    id = node.attr('id')&.to_s
    author = node.attr('author')&.to_s
    context = node.attr('context')&.to_s
    on(errors, file) do
      demand(id, 'ID is empty')
      confirm(id, /[-a-z]+/, "ID #{id.inspect} has not suffix in #{context} context") if context
    end
    on(errors, file) do
      demand(author, 'author is empty')
      confirm(author, /\A[-_ A-Za-z0-9]+\z/, "author #{author.inspect} has illegal symbols")
    end
    on(errors, file) do
      demand(id, 'ID is empty')
      demand(path, 'logicalFilePath is empty')
      confirm(
        path.gsub(/[-_.a-z]/, ''),
        /\A#{id.gsub(/[-a-z]/, '')}\z/,
        "ID #{id.inspect} is not the beginning of a logicalFilePath #{path.inspect}"
      )
    end
  end

  def report(errors)
    return if errors.empty?
    puts('There are such errors in the Liquibase XML files.')
    errors.each do |f, e|
      puts("In file '#{f}':")
      e.uniq.each do |msg|
        puts("  * #{msg}")
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

  def demand(prop, msg)
    raise(MustError, msg) if prop.nil? || prop.empty?
  end

  def equate(lprop, rprop, msg)
    raise(MustError, msg) if lprop != rprop
  end

  def confirm(prop, regex, msg)
    raise(MustError, msg) unless prop.match?(regex)
  end

  class MustError < StandardError
  end
  private_constant :MustError
end
