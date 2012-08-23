require 'test_helper'
require 'test/unit'

class ReplicateModel < ActiveRecord::Base
  data_fabric :replicated => true, :dynamic_toggle => true
end

class AnotherReplicateModel < ActiveRecord::Base
  data_fabric :replicated => true, :dynamic_toggle => true
end

class NormalModel < ActiveRecord::Base
end

class PollerMock
  def initialize(behind)
    @behind = behind
  end

  def behind?
    @behind
  end

  def check_server?
    true
  end
end

class TimedPollerMock
  def initialize(interval = 2)
    @checked = Time.now
    @interval = interval
  end

  def check_server?
    time = Time.now
    if time > @checked + @interval
      @checked = time
      return true
    end
    false
  end

  def behind?
    true
  end
end

class DynamicSwitchingTest < Test::Unit::TestCase
  def setup
    ActiveRecord::Base.configurations = @settings = load_database_yml
  end

  def test_reads_from_slave_when_below_threshold
    ReplicateModel.connection.status_checker.poller = PollerMock.new(false)
    assert_equal "test_slave", ReplicateModel.find(1).name
  end

  def test_reads_from_master_when_above_threshold
    ReplicateModel.connection.status_checker.poller = PollerMock.new(true)
    assert_equal "test_master", ReplicateModel.find(1).name
  end

  def test_with_master_always_goes_to_master
    ReplicateModel.connection.status_checker.poller = PollerMock.new(false)
    assert_equal "test_master", ReplicateModel.with_master() { ReplicateModel.find(1).name }
  end

  def test_with_master_can_be_nested
    ReplicateModel.connection.status_checker.poller        = PollerMock.new(false)
    AnotherReplicateModel.connection.status_checker.poller = PollerMock.new(false) # This overwrites the poller set above (it's the same connection).

    result = ReplicateModel.with_master do
      ReplicateModel.find(1).name + ReplicateModel.with_master { ReplicateModel.find(1).name } + AnotherReplicateModel.with_master { AnotherReplicateModel.find(1).name}
    end
    assert_equal "test_mastertest_mastertest_master", result
  end

  def test_with_slave_can_be_nested
    ReplicateModel.connection.status_checker.poller = PollerMock.new(true)

    result = ReplicateModel.first.name + ReplicateModel.with_slave do
      ReplicateModel.with_slave { ReplicateModel.first.name } + ReplicateModel.first.name
    end + ReplicateModel.first.name
    assert_equal("test_mastertest_slavetest_slavetest_master", result, "Slave should not swap when in a fixed role block.")
  end


  def test_find_in_batches_doesnt_swap_during_a_find_when_inside_current_db
    ReplicateModel.connection.status_checker.poller = TimedPollerMock.new(1)
    ReplicateModel.with_current_db { ReplicateModel.find_in_batches(:batch_size => 1) { |batch| sleep 0.5; assert_equal "test_slave", batch.first.name } }
  end
end
