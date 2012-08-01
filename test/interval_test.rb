class IntervalTest < Test::Unit::TestCase
  def setup
    ActiveRecord::Base.configurations = @settings = load_database_yml
  end

  def test_it_should_read_the_check_interval_from_the_slave_in_the_yml_file
    assert_equal @settings["test_slave"]["check_interval"], DataFabricDynamicSwitching::Interval.instance.check_interval
  end
  
  def test_it_should_read_the_threshold_from_the_slave_in_the_yml_file
    assert_equal @settings["test_slave"]["delay_threshold"], DataFabricDynamicSwitching::Interval.instance.threshold
  end
end
