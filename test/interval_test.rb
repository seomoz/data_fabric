require 'test_helper'

class ReplicateModel < ActiveRecord::Base
  data_fabric :replicated => true, :dynamic_toggle => true
end

class IntervalTest < Test::Unit::TestCase
  def test_reads_from_slave_when_below_threshold
    ActiveRecord::Base.configurations = load_database_yml
    
    flexmock(DataFabricInterval.instance).should_receive(:threshold).and_return(5)
    flexmock(DataFabricInterval.instance).should_receive(:seconds_behind).and_return(4)
    flexmock(DataFabricInterval.instance).should_receive(:check_server?).and_return(true)
    assert_equal "test_slave", ReplicateModel.find(1).name
  end
  
  def test_reads_from_master_when_above_threshold
    ActiveRecord::Base.configurations = load_database_yml
    flexmock(DataFabricInterval.instance).should_receive(:threshold).and_return(1)
    flexmock(DataFabricInterval.instance).should_receive(:seconds_behind).and_return(4)
    flexmock(DataFabricInterval.instance).should_receive(:check_server?).and_return(true)
    assert_equal "test_master", ReplicateModel.find(1).name
  end
  
  def test_with_master_always_goes_to_master
    ActiveRecord::Base.configurations = load_database_yml
    flexmock(DataFabricInterval.instance).should_receive(:threshold).and_return(5)
    flexmock(DataFabricInterval.instance).should_receive(:seconds_behind).and_return(4)
    flexmock(DataFabricInterval.instance).should_receive(:check_server?).and_return(true)
    assert_equal "test_master", ReplicateModel.with_master() { ReplicateModel.find(1).name }
  end
  
  def test_seconds_behind_should_return_no_lag_for_an_improper_setup
    ActiveRecord::Base.configurations = load_database_yml
    assert_equal 0, DataFabricInterval.instance.seconds_behind
  end
  
  def test_it_should_read_the_check_interval_from_the_slave_in_the_yml_file
    settings = load_database_yml
    assert_equal settings["test_slave"]["check_interval"], DataFabricInterval.instance.check_interval
  end
  
  def test_it_should_read_the_threshold_from_the_slave_in_the_yml_file
    settings = load_database_yml
    assert_equal settings["test_slave"]["delay_threshold"], DataFabricInterval.instance.threshold
  end
  
  def test_it_should_not_change_the_connection_for_non_replicated_classes
    ActiveRecord::Base.configurations = load_database_yml
    ActiveRecord::Base.establish_connection :test_master
    assert NormalModel.first.name =~ /master/
    DataFabricInterval.instance.seconds_behind
    assert NormalModel.first.name =~ /master/
  end
end