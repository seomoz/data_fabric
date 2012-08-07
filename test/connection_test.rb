require 'test_helper'
require 'test/unit'
require 'flexmock'
require 'active_record/connection_adapters/mysql2_adapter'

class PrefixModel < ActiveRecord::Base
  data_fabric :prefix => 'prefix'
end

class ShardModel < ActiveRecord::Base
  data_fabric :shard_by => :city
end

class TheWholeEnchilada < ActiveRecord::Base
  data_fabric :prefix => 'fiveruns', :replicated => true, :shard_by => :city
end

class AdapterMock < ActiveRecord::ConnectionAdapters::AbstractAdapter
  # Minimum required to perform a find with no results.
  def columns(table_name, name=nil)
    [ActiveRecord::ConnectionAdapters::Column.new('id', 0, :integer, false)]
  end
  def primary_key(name)
    :id
  end
  def adapter_name
    'mysql2'
  end
  def select(sql, name=nil, bindings=nil)
    []
  end
  def execute(sql, name=nil)
    []
  end
  def tables
    ["enchiladas", "the_whole_burritos", "replicate_models"]
  end
  def table_exists?(name)
    true
  end
  def last_inserted_id(result)
    1
  end
  def method_missing(name, *args)
    raise ArgumentError, "#{self.class.name} missing '#{name}': #{args.inspect}"
  end

  def self.visitor_for(pool)
    Arel::Visitors::MySQL.new(pool)
  end
end

class RawConnection
  def method_missing(name, *args)
    puts "#{self.class.name} missing '#{name}': #{args.inspect}"
  end
end

class ConnectionTest < Test::Unit::TestCase
  include FlexMock::TestCase

  def test_should_install_into_arbase
    assert PrefixModel.methods.map(&:to_s).include?('data_fabric')
  end
  
  def test_prefix_connection_name
    setup_configuration_for PrefixModel, 'prefix_test'
    assert_equal 'prefix_test', PrefixModel.connection.connection_name
  end
  
  def test_shard_connection_name
    setup_configuration_for ShardModel, 'city_austin_test'
    # ensure unset means error
    assert_raises ArgumentError do
      ShardModel.connection.connection_name
    end
    DataFabric.activate_shard(:city => 'austin', :category => 'art') do
      assert_equal 'city_austin_test', ShardModel.connection.connection_name
    end
    # ensure it got unset
    assert_raises ArgumentError do
      ShardModel.connection.connection_name
    end
  end
  
  def test_respond_to_connection_methods
    setup_configuration_for ShardModel, 'city_austin_test'
    DataFabric.activate_shard(:city => 'austin', :category => 'art') do
      assert ShardModel.connection.respond_to?(:columns)
      assert ShardModel.connection.respond_to?(:primary_key)
      assert !ShardModel.connection.respond_to?(:nonexistent_method)
    end
  end
  
  def test_respond_to_connection_proxy_methods
    setup_configuration_for ShardModel, 'city_austin_test'
    DataFabric.activate_shard(:city => 'austin', :category => 'art') do
      assert ShardModel.connection.respond_to?(:with_master)
      assert !ShardModel.connection.respond_to?(:nonexistent_method)
    end
  end
  
  def test_enchilada
    setup_configuration_for TheWholeEnchilada, 'fiveruns_city_dallas_test_slave'
    setup_configuration_for TheWholeEnchilada, 'fiveruns_city_dallas_test_master'
  
    DataFabric.activate_shard :city => :dallas do
      assert_equal 'fiveruns_city_dallas_test_slave', TheWholeEnchilada.connection.connection_name
  
      # Should use the slave
      assert_raises ActiveRecord::RecordNotFound do
        TheWholeEnchilada.find(1)
      end
  
      # Should use the master
      mmmm = TheWholeEnchilada.new
      mmmm.instance_variable_set(:@attributes, { 'id' => 1 })
      assert_raises ActiveRecord::RecordNotFound do
        mmmm.reload
      end
      # ...but immediately set it back to default to the slave
      assert_equal 'fiveruns_city_dallas_test_slave', TheWholeEnchilada.connection.connection_name
  
      # Should use the master
      TheWholeEnchilada.transaction do
        mmmm.save!
      end
      TheWholeEnchilada.verify_active_connections!
      TheWholeEnchilada.clear_active_connections!
      TheWholeEnchilada.clear_all_connections!
    end
  
    TheWholeEnchilada.verify_active_connections!
    TheWholeEnchilada.clear_active_connections!
    TheWholeEnchilada.clear_all_connections!
  end
    
  private

  def setup_configuration_for(clazz, name)
    flexmock(ActiveRecord::Base).should_receive(:mysql2_connection).and_return(AdapterMock.new(RawConnection.new))
    ActiveRecord::Base.configurations ||= HashWithIndifferentAccess.new
    ActiveRecord::Base.configurations[name] = HashWithIndifferentAccess.new({ :adapter => 'mysql2', :database => name, :host => 'localhost'})
  end
end
