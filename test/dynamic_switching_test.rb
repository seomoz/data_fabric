require 'test_helper'

class ReplicateModel < ActiveRecord::Base
  data_fabric :replicated => true, :dynamic_toggle => true
end

class CheckerMock
  def initialize(seconds_behind = 5)
    @seconds_behind = seconds_behind
  end
  
  def seconds_behind
    @seconds_behind
  end
  
  def behind?(threshold)
    seconds_behind > threshold
  end
end

class DynamicSwitchingTest < Test::Unit::TestCase
  def setup
    ActiveRecord::Base.configurations = @settings = load_database_yml
  end
  
  def test_reads_from_slave_when_below_threshold
    flexmock(DataFabricDynamicSwitching::Interval.instance).should_receive(:threshold).and_return(5)
    flexmock(DataFabricDynamicSwitching::Interval.instance).should_receive(:check_server?).and_return(true)
    DataFabricDynamicSwitching::Interval.instance.checker = CheckerMock.new(4)
    
    assert_equal "test_slave", ReplicateModel.find(1).name
  end
  
  def test_reads_from_master_when_above_threshold
    flexmock(DataFabricDynamicSwitching::Interval.instance).should_receive(:threshold).and_return(1)
    flexmock(DataFabricDynamicSwitching::Interval.instance).should_receive(:check_server?).and_return(true)
    DataFabricDynamicSwitching::Interval.instance.checker = CheckerMock.new(4)
    
    assert_equal "test_master", ReplicateModel.find(1).name
  end
  
  def test_with_master_always_goes_to_master
    flexmock(DataFabricDynamicSwitching::Interval.instance).should_receive(:threshold).and_return(5)
    flexmock(DataFabricDynamicSwitching::Interval.instance).should_receive(:check_server?).and_return(true)
    DataFabricDynamicSwitching::Interval.instance.checker = CheckerMock.new(4)
    
    assert_equal "test_master", ReplicateModel.with_master() { ReplicateModel.find(1).name }
  end
  
  def test_it_should_not_change_the_connection_for_non_replicated_classes  
    ActiveRecord::Base.establish_connection :test_master
    assert NormalModel.first.name =~ /master/
    DataFabricDynamicSwitching::Interval.instance.behind?
    assert NormalModel.first.name =~ /master/
  end
end