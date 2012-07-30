require 'data_fabric/connection_proxy'

class ActiveRecord::ConnectionAdapters::ConnectionHandler
  def clear_active_connections_with_data_fabric!
    clear_active_connections_without_data_fabric!
    DataFabric::ConnectionProxy.shard_pools.each_value { |pool| pool.release_connection }
  end
  alias_method_chain :clear_active_connections!, :data_fabric
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
