# frozen_string_literal: true

# SPDX-FileCopyrightText: Copyright (c) 2019-2025 Yegor Bugayenko
# SPDX-License-Identifier: MIT

require 'English'

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require_relative 'lib/pgtk/version'
Gem::Specification.new do |s|
  s.required_rubygems_version = Gem::Requirement.new('>= 0') if s.respond_to? :required_rubygems_version=
  s.required_ruby_version = '>= 2.3'
  s.name = 'pgtk'
  s.version = Pgtk::VERSION
  s.license = 'MIT'
  s.summary = 'PostgreSQL ToolKit for Ruby apps'
  s.description =
    'This small Ruby gem helps you integrate PostgreSQL ' \
    'with your Ruby web app, through Liquibase. It also adds a simple ' \
    'connection pool and query processor, to make SQL manipulation simpler.'
  s.authors = ['Yegor Bugayenko']
  s.email = 'yegor256@gmail.com'
  s.homepage = 'https://github.com/yegor256/pgtk'
  s.files = `git ls-files`.split($RS)
  s.executables = s.files.grep(%r{^bin/}) { |f| File.basename(f) }
  s.rdoc_options = ['--charset=UTF-8']
  s.extra_rdoc_files = ['README.md', 'LICENSE.txt']
  s.add_dependency 'backtrace', '~>0.4'
  s.add_dependency 'concurrent-ruby', '~>1.3'
  s.add_dependency 'joined', '~>0.3'
  s.add_dependency 'logger', '~>1.7'
  s.add_dependency 'loog', '~>0.6'
  s.add_dependency 'pg', '~>1.1'
  s.add_dependency 'qbash', '~>0.4'
  s.add_dependency 'random-port', '~>0.7'
  s.add_dependency 'tago', '~>0.1'
  s.metadata['rubygems_mfa_required'] = 'true'
end
