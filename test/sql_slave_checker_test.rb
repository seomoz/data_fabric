class SQLSlaveCheckerTest < Test::Unit::TestCase
  def setup
    @slave_checker = DataFabricDynamicSwitching::SQLSlaveChecker.new("test_slave")
  end
  
  def test_seconds_behind_should_return_no_lag_for_an_improper_setup
    assert_equal 0, @slave_checker.seconds_behind
  end
  
  def test_it_should_not_change_the_connection_for_non_replicated_classes  
    ActiveRecord::Base.establish_connection :test_master
    assert NormalModel.first.name =~ /master/
    @slave_checker.seconds_behind
    assert NormalModel.first.name =~ /master/
  end
end
