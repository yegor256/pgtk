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
    @errors = {}
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
    Dir[File.join(File.expand_path(File.join(Dir.pwd, @dir)), @pattern)].each do |f|
      with_file(f) do
        doc = Nokogiri::XML(File.open(f))
        doc.remove_namespaces!
        path = decent(doc.at_xpath('databaseChangeLog')&.attr('logicalFilePath')&.to_s, 'logicalFilePath')
        doc.xpath('databaseChangeLog/changeSet').each do |node|
          id = decent(node.attr('id')&.to_s, 'id')
          author = decent(node.attr('author')&.to_s, 'author')
          context = node.attr('context')&.to_s
          present(author) do
            no_match(author, /\A[-_ A-Za-z0-9]+\z/) do
              error!("author '#{author}' has illegal symbols#{' in test context' if context == 'test'}")
            end
          end
          if context == 'test'
            present(id) do
              no_match(id, /[-a-z]+/) do
                error!("id '#{id}' has not suffix in test context")
              end
            end
            present(id, path) do
              match(id, /[-a-z]+/) do
                no_match(path.gsub(/[-_.a-z]/, ''), /\A#{id.gsub(/[-a-z]/, '')}\z/) do
                  error!("id '#{id}' is not the beginning of a logicalFilePath '#{path}' in test context")
                end
              end
            end
          else
            present(id, path) do
              no_match(path.gsub(/[-_.a-z]/, ''), /\A#{id}\z/) do
                error!("id '#{id}' is not the beginning of a logicalFilePath '#{path}'")
              end
            end
          end
        end
        present(path) do
          no_equal(path, File.basename(f)) do
            error!("logicalFilePath '#{path}' does not match the xml file name '#{File.basename(f)}'")
          end
        end
      end
    end
    return if @errors.empty?
    puts 'There are such errors in the Liquibase XML files.'
    @errors.each do |f, e|
      puts "In file '#{f}':"
      e.each do |msg|
        puts "  * #{msg}"
      end
      puts
    end
    exit(1)
  end

  def with_file(file, &)
    @f = file
    yield
  ensure
    @f = nil
  end

  def error!(msg)
    return if @f.nil?
    (@errors[@f] ||= []) << msg
  end

  def decent(prop, name)
    if prop.nil?
      error!("#{name} is nil")
    elsif prop.empty?
      error!("#{name} is empty")
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
end
