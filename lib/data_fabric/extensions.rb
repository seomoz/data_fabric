require 'data_fabric/connection_proxy'
require 'singleton'

class ActiveRecord::ConnectionAdapters::ConnectionHandler
  def clear_active_connections_with_data_fabric!
    clear_active_connections_without_data_fabric!
    DataFabric::ConnectionProxy.shard_pools.each_value { |pool| pool.release_connection }
  end
  alias_method_chain :clear_active_connections!, :data_fabric
end

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
    ActiveRecord::Base.establish_connection "#{environment}_slave"
    return 0 unless ActiveRecord::Base.connection.adapter_name =~ /mysql/i

    result = ActiveRecord::Base.connection.execute "SHOW SLAVE STATUS;"
    result.to_a.last.first.split(//).last.to_i rescue 0
  end
  
  def behind?
    seconds_behind > threshold
  end
end

class DataFabricStatus
  include Singleton
  
  def initialize
    @master = false
  end
  
  def master?
    @master
  end
    
  def update_status
    return unless DataFabricInterval.instance.check_server?
    @master = DataFabricInterval.instance.behind?
  end
end

module DataFabric
  module Extensions
        
    def self.included(model)
      DataFabric.logger.info { "Loading data_fabric #{DataFabric::Version::STRING} with ActiveRecord #{ActiveRecord::VERSION::STRING}" }
            
      # Wire up ActiveRecord::Base
      model.extend ClassMethods
      ConnectionProxy.shard_pools = {}
    end

    # Class methods injected into ActiveRecord::Base
    module ClassMethods
      def data_fabric(options)
        DataFabric.logger.info { "Creating data_fabric proxy for class #{name}" }
        connection_handler.connection_pools[name] = PoolProxy.new(ConnectionProxy.new(self, options))
      end
      
      def with_master(&block)
        connection_handler.connection_pools[name].connection.with_master(&block)
      end
    end
  end
end