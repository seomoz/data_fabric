require 'singleton'
module DataFabricDynamicSwitching  
  
  def self.environment
    (defined?(Rails) && Rails.env) || ENV["RAILS_ENV"] || "test"
  end
  
  class Status
    include Singleton
  
    def initialize
      @master = false
    end
  
    def master?
      @master
    end
    
    def update_status
      return unless Interval.instance.check_server?
      @master =     Interval.instance.behind?
    end
  end
  
  class Interval
    include Singleton
    attr_reader :threshold, :check_interval
    attr_accessor :checker
  
    def initialize
      @last_checked   = Time.now
      self.checker    = SQLSlaveChecker.new
      environment     = DataFabricDynamicSwitching.environment
    
      yml_file = environment == "test" ? File.join(ROOT_PATH, "test", "database.yml")
                     : defined?(Rails) ? Rails.root.join("config", "database.yml") 
                                       : File.join(RAILS_ROOT, "config", "database.yml")
  
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
    
    def behind?
      checker.behind?(threshold)
    end
  end

  class SQLSlaveChecker
    def seconds_behind
      return 0 unless slave_pool.connection.adapter_name =~ /mysql/i
      result = slave_pool.connection.execute "SHOW SLAVE STATUS;"
      result.to_a.last.first.split(//).last.to_i rescue 0
    end
    
    def behind?(threshold)
      seconds_behind > threshold
    end
    
    private
    
    def name
      "#{DataFabricDynamicSwitching.environment}_slave"
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
end

