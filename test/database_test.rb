require 'test_helper'
require 'test/unit'

class TheWholeBurrito < ActiveRecord::Base
  data_fabric :prefix => 'fiveruns', :replicated => true, :shard_by => :city
end

class DatabaseTest < Test::Unit::TestCase
  
  def setup
    ActiveRecord::Base.configurations = load_database_yml
    DataFabric::ConnectionProxy.shard_pools.clear
  end

  def test_features
    DataFabric.activate_shard :city => :dallas do
      assert_equal 'fiveruns_city_dallas_test_slave', TheWholeBurrito.connection.connection_name
      assert_equal DataFabric::PoolProxy, TheWholeBurrito.connection_pool.class
      assert !TheWholeBurrito.connected?

      # Should use the slave
      burrito = TheWholeBurrito.find(1)
      assert_match 'vr_dallas_slave', burrito.name

      assert TheWholeBurrito.connected?
    end
  end

  def test_live_burrito
    DataFabric.activate_shard :city => :dallas do
      assert_equal 'fiveruns_city_dallas_test_slave', TheWholeBurrito.connection.connection_name

      # Should use the slave
      burrito = TheWholeBurrito.find(1)
      assert_match 'vr_dallas_slave', burrito.name

      # Should use the master
      burrito.reload
      assert_match 'vr_dallas_master', burrito.name

      # ...but immediately set it back to default to the slave
      assert_equal 'fiveruns_city_dallas_test_slave', TheWholeBurrito.connection.connection_name

      # Should use the master
      TheWholeBurrito.transaction do
        burrito = TheWholeBurrito.find(1)
        assert_match 'vr_dallas_master', burrito.name
        burrito.name = 'foo'
        burrito.save!
      end
    end
  end
end
