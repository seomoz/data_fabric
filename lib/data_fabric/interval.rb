require 'singleton'
class DataFabricInterval
  include Singleton
  attr_reader :threshold, :check_interval
  
  def initialize
    @last_checked   = Time.now
    
    if environment == "test"
      yml_file = File.join(ROOT_PATH, "test", "database.yml")
    elsif defined?(Rails)
      yml_file = Rails.root.join("config", "database.yml")
    else
      yml_file = File.join(RAILS_ROOT, "config", "database.yml")
    end
  
    settings = YAML::load(ERB.new(IO.read(yml_file)).result)
    
    @check_interval = (settings["#{environment}_slave"] && settings["#{environment}_slave"]["check_interval"])  || 5
    @threshold      = (settings["#{environment}_slave"] && settings["#{environment}_slave"]["delay_threshold"]) || 5
  end
  
  def check_server?
    time = Time.now
    if time > @last_checked + @check_interval
      @last_checked = time
      return true
    end
    false
  end
  
  def environment
    (defined?(Rails) && Rails.env) || ENV["RAILS_ENV"] || "test"
  end
  
  def seconds_behind
    return 0 unless slave_pool.connection.adapter_name =~ /mysql/i
    result = slave_pool.connection.execute "SHOW SLAVE STATUS;"
    result.to_a.last.first.split(//).last.to_i rescue 0
  end
  
  def behind?
    seconds_behind > threshold
  end
  
  def name
    "#{environment}_slave"
  end
  
  def slave_pool
    config = ActiveRecord::Base.configurations[name]
    raise ArgumentError, "Unknown database config: #{name}" unless config
    ActiveRecord::ConnectionAdapters::ConnectionPool.new(spec_for(config))
  end
  
  def spec_for(config)
    config = config.symbolize_keys
    adapter_method = "#{config[:adapter]}_connection"
    ActiveRecord::Base::ConnectionSpecification.new(config, adapter_method)
  end
end