require 'fileutils'
include FileUtils::Verbose

require 'rake/testtask'
Rake::TestTask.new do |t|
  t.verbose = true
  t.libs << 'test'
  t.test_files = FileList['test/*_test.rb']
end

task :clean do
  rm_f Dir['*.gem']
  rm_f Dir['test/*.db']
  rm_rf 'coverage'

  puts `cd example23 ; rake app:clean`
end

task :default => :test
task :test => [:pretest]

desc "Test all versions of ActiveRecord installed locally"
task :test_all do
  Gem.source_index.search(Gem::Dependency.new('activerecord', '>=2.0')).each do |spec|
    puts `rake test AR_VERSION=#{spec.version}`
  end
end

task :pretest do
  setup(false)
end

task :create_db do
  setup(true)
end

def load_database_yml
  filename = "test/database.yml"
  if !File.exist?(filename)
    STDERR.puts "\n*** ERROR ***:\n" <<
      "You must have a 'test/database.yml' file in order to create the test database. " <<
      "An example is provided in 'test/database.yml.mysql'.\n\n"
    exit 1
  end
  YAML::load(ERB.new(IO.read(filename)).result)
end

def setup_connection
  require 'erb'
  require 'logger'
  require 'active_record'
  ActiveRecord::Base.configurations = load_database_yml
  ActiveRecord::Base.logger = Logger.new(STDOUT)
  ActiveRecord::Base.logger.level = Logger::INFO
end

def using_connection(database_identifier, &block)
  ActiveRecord::Base.establish_connection(database_identifier)
  ActiveRecord::Base.connection.instance_eval(&block)
end

def setup(create = false)
  setup_connection
  
  ActiveRecord::Base.configurations.each_pair do |identifier, config|
    using_connection(identifier) do
      send("create_#{config['adapter']}", create, config['database'])
    end  
  end
end

def create_sqlite3(create, db_name)
  db_name = File.basename(db_name).gsub(/(\..*)/, "")
  execute "drop table if exists the_whole_burritos"
  execute "drop table if exists enchiladas"
  execute "drop table if exists replicate_models"
  execute "drop table if exists another_replicate_models"
  execute "drop table if exists normal_models"
  execute "create table enchiladas (id integer not null primary key, name varchar(30) not null)"
  execute "insert into enchiladas (id, name) values (1, '#{db_name}')"
  execute "create table the_whole_burritos (id integer not null primary key, name varchar(30) not null)"
  execute "insert into the_whole_burritos (id, name) values (1, '#{db_name}')"
  execute "create table replicate_models (id integer not null primary key, name varchar(30) not null)"
  execute "create table another_replicate_models (id integer not null primary key, name varchar(30) not null)"
  execute "insert into replicate_models (id, name) values (1, '#{db_name}')"
  execute "insert into replicate_models (id, name) values (2, '#{db_name}')"
  execute "insert into replicate_models (id, name) values (3, '#{db_name}')"
  execute "insert into replicate_models (id, name) values (4, '#{db_name}')"
  execute "insert into another_replicate_models (id, name) values (1, '#{db_name}')"
  execute "create table normal_models (id integer not null primary key, name varchar(30) not null)"
  execute "insert into normal_models (id, name) values (1, '#{db_name}')"
end

def create_mysql(create, db_name)
  db_name = File.basename(db_name).gsub(/(\..*)/, "")
  
  if create
    execute "drop database if exists #{db_name}"
    execute "create database #{db_name}"
  end
  execute "use #{db_name}"
  execute "drop table if exists the_whole_burritos"
  execute "drop table if exists enchiladas"
  execute "drop table if exists replicate_models"
  execute "drop table if exists another_replicate_models"
  execute "drop table if exists normal_models"
  execute "create table enchiladas (id integer not null auto_increment, name varchar(30) not null, primary key(id))"
  execute "insert into enchiladas (id, name) values (1, '#{db_name}')"
  execute "create table the_whole_burritos (id integer not null auto_increment, name varchar(30) not null, primary key(id))"
  execute "insert into the_whole_burritos (id, name) values (1, '#{db_name}')"
  execute "create table replicate_models (id integer not null auto_increment, name varchar(30) not null, primary key(id))"
  execute "create table another_replicate_models (id integer not null auto_increment, name varchar(30) not null, primary key(id))"
  execute "insert into replicate_models (id, name) values (1, '#{db_name}')"
  execute "insert into replicate_models (id, name) values (2, '#{db_name}')"
  execute "insert into replicate_models (id, name) values (3, '#{db_name}')"
  execute "insert into replicate_models (id, name) values (4, '#{db_name}')"
  execute "insert into another_replicate_models (id, name) values (1, '#{db_name}')"
  execute "create table normal_models (id integer not null primary key, name varchar(30) not null)"
  execute "insert into normal_models (id, name) values (1, '#{db_name}')"
end

# Test coverage
begin
  gem 'rcov' rescue nil
  require 'rcov/rcovtask'

  desc "Generate coverage numbers for all locally installed versions of ActiveRecord"
  task :cover_all do
    Gem.source_index.search(Gem::Dependency.new('activerecord', '>=2.0')).each do |spec|
      puts `rake cover AR_VERSION=#{spec.version}`
    end
  end

  task :cover => [:pretest, :rcov_impl]

  Rcov::RcovTask.new('rcov_impl') do |t|
    t.libs << "test"
    t.test_files = FileList["test/*_test.rb"]
    t.output_dir = "coverage/#{ENV['AR_VERSION']}"
    t.verbose = true
    t.rcov_opts = ['--text-report', '--exclude', "test,Library,#{ENV['GEM_HOME']}", '--sort', 'coverage']
  end
rescue LoadError => e
  puts 'Test coverage support requires \'gem install rcov\''
end
