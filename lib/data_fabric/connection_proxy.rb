module DataFabric
  module ActiveRecordConnectionMethods
    def self.included(base)
      base.alias_method_chain :reload, :master
    end

    def reload_with_master(*args, &block)
      connection.with_master { reload_without_master }
    end
  end

  class StringProxy
    def initialize(&block)
      @proc = block
    end
    def to_s
      @proc.call
    end
  end
  
  class PoolProxy
    def initialize(proxy)
      @proxy = proxy
    end

    def connection
      @proxy
    end

    def spec
      @proxy.spec
    end

    def with_connection
      yield @proxy
    end

    def connected?
      @proxy.connected?
    end

    %w(disconnect! release_connection clear_reloadable_connections! clear_stale_cached_connections! verify_active_connections!).each do |name|
      define_method(name.to_sym) do
        @proxy.shard_pools.values.each do |pool|
          pool.send(name.to_sym)
        end
      end
    end
  end

  class ConnectionProxy
    cattr_accessor :shard_pools
    attr_accessor :spec
    
    def initialize(model_class, options)
      @model_class =  model_class
      @replicated  =  options[:replicated]
      @shard_group =  options[:shard_by]
      @prefix      =  options[:prefix]
      @default_role = options[:default_role] || 'slave'
      set_role(@default_role) if @replicated

      @model_class.send :include, ActiveRecordConnectionMethods if @replicated
    end

    delegate :insert, :update, :delete, :create_table, :rename_table, :drop_table, :add_column, :remove_column, 
      :change_column, :change_column_default, :rename_column, :add_index, :remove_index, :initialize_schema_information,
      :dump_schema_information, :execute, :execute_ignore_duplicate, :to => :master

    delegate :insert_many, :to => :master # ar-extensions bulk insert support

    def transaction(start_db_transaction = true, &block)
      with_master do
        connection.transaction(start_db_transaction, &block) 
      end
    end

    def method_missing(method, *args, &block)
      DataFabric.logger.debug { "Calling #{method} on #{connection}" }
      connection.send(method, *args, &block)
    end

    def connection_name
      connection_name_builder.join('_')
    end

    def with_master(&block)
      with_role('master', &block)
    end

    def with_slave(&block)
      with_role('slave', &block)
    end

    def with_role(role)
      # Allow nesting of with_role.
      old_role = current_role
      set_role(role)
      yield
    ensure
      set_role(old_role)
    end
    
    def connected?
      current_pool.connected?
    end

    def connection
      current_pool.connection
    end

    def pk_and_sequence_for(*args)
      connection.send(:pk_and_sequence_for, *args)
    end

    def primary_key(*args)
      connection.send(:primary_key, *args)
    end

    def shard_names
      @shard_names ||= begin
        clauses = []
        clauses << @prefix if @prefix
        clauses << @shard_group if @shard_group
        clauses << "([^_]+)"
        clauses << RAILS_ENV
        clauses << 'master' if @replicated
        regex = %r{#{clauses.join("_")}}
        ActiveRecord::Base.configurations.keys.map do |conn_name|
          md = regex.match(conn_name)
          md && md[1]
        end.compact
      end
    end

  private

    def in_transaction?
      current_role == 'master'
    end

    def current_pool
      name = connection_name
      self.class.shard_pools[name] ||= begin
        config = ActiveRecord::Base.configurations[name]
        raise ArgumentError, "Unknown database config: #{name}, have #{ActiveRecord::Base.configurations.inspect}" unless config
        ActiveRecord::ConnectionAdapters::ConnectionPool.new(spec_for(config))
      end
    end
    
    def spec_for(config)
      # XXX This looks pretty fragile.  Will break if AR changes how it initializes connections and adapters.
      config = config.symbolize_keys
      adapter_method = "#{config[:adapter]}_connection"
      initialize_adapter(config[:adapter])
      @spec = ActiveRecord::Base::ConnectionSpecification.new(config, adapter_method)
    end
    
    def initialize_adapter(adapter)
      begin
        require 'rubygems'
        gem "activerecord-#{adapter}-adapter"
        require "active_record/connection_adapters/#{adapter}_adapter"
      rescue LoadError
        begin
          require "active_record/connection_adapters/#{adapter}_adapter"
        rescue LoadError
          raise "Please install the #{adapter} adapter: `gem install activerecord-#{adapter}-adapter` (#{$!})"
        end
      end
    end      

    def connection_name_builder
      @connection_name_builder ||= begin
        clauses = []
        clauses << @prefix if @prefix
        clauses << @shard_group if @shard_group
        clauses << StringProxy.new { DataFabric.active_shard(@shard_group) } if @shard_group
        clauses << RAILS_ENV
        clauses << StringProxy.new { current_role } if @replicated
        clauses
      end
    end
    
    def set_role(role)
      Thread.current[:data_fabric_role] = role
    end
    
    def current_role
      Thread.current[:data_fabric_role] || @default_role
    end

    def master
      with_master { return connection }
    end
  end

end
