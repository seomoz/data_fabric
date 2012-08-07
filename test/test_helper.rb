ENV['RAILS_ENV'] = 'test'
ROOT_PATH = File.expand_path(File.join(File.dirname(__FILE__), ".."))
DATABASE_YML_PATH = File.join(ROOT_PATH, "test", "database.yml")
Dir.chdir(ROOT_PATH)

require 'rubygems'
require 'minitest/unit'
require 'minitest/autorun'
require 'minitest/pride'
require 'erb'
require 'logger'

version = ENV['AR_VERSION']
if version
  puts "Testing ActiveRecord #{version}"
  gem 'activerecord', "=#{version}"
end

require 'active_record'
require 'active_record/version'
ActiveRecord::Base.logger = Logger.new(STDOUT)
ActiveRecord::Base.logger.level = Logger::WARN

require 'data_fabric'

DataFabricDynamicSwitching.load_configurations( File.join(File.dirname(__FILE__), "database.yml") )

def load_database_yml
  filename = DATABASE_YML_PATH
  YAML::load(ERB.new(IO.read(filename)).result)
end
