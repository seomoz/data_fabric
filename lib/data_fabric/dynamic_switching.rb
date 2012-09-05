module DataFabric::DynamicSwitching
  def self.environment
    (defined?(Rails) && Rails.env) || ENV["RAILS_ENV"] || "test"
  end

  def self.statuses
    @statuses
  end

  def self.statuses=(arg)
    @statuses = arg
  end

  def self.status_for(db_configuration)
    if statuses[db_configuration]
      return statuses[db_configuration]
    else
      raise ArgumentError, "Bad DB configuration #{db_configuration}"
    end
  end

  def self.configurations
    configurations = configs_from_yml(@yml_file)
  end

  def self.configs_from_yml(file)
    settings = YAML::load(ERB.new(IO.read(file)).result)
    configs  = settings.collect { |key, value| value.merge({:name => key}) }\
    .select {|config| config[:name] =~ /slave/}\
    .map {|config| config.delete_if {|key, value| key.to_s !~ /check_interval|delay_threshold|name/} }
  end

  def self.load_configurations(file)
    self.statuses  = {}
    @yml_file      = file

    configurations.each do |config|
      status                        = Status.new config
      self.statuses[config[:name]]  = status
    end
  end

  class Status
    attr_accessor :poller

    def initialize(config)
      self.master = false
      self.poller = Poller.new(config)
      @name       = config[:name]
    end

    def master?
      master
    end

    def master
      Thread.current["#{@name}_status"]
    end

    def master=(arg)
      Thread.current["#{@name}_status"] = arg
    end

    def update_status
      return unless poller.check_server?
      self.master = poller.behind?
    end

    def slave_connection=(connection)
      self.poller.slave_connection = connection
    end
  end

  class Poller
    attr_accessor :checker

    def initialize(config)
      @last_checked   = Time.now
      self.checker    = SQLSlaveChecker.new config[:name]

      @check_interval = config["check_interval"]  || 5
      @threshold      = config["delay_threshold"] || 5
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
      checker.behind?(@threshold)
    end

    def slave_connection=(connection)
      self.checker.slave_connection = connection
    end
  end

  class SQLSlaveChecker
    def initialize(name)
      @name = name
    end

    def slave_connection=(connection)
      @connection ||= connection
    end

    def seconds_behind
      return 0 unless @connection && @connection.adapter_name =~ /mysql/i
      result = @connection.execute "SHOW SLAVE STATUS;"
      result.to_a.last.first.split(//).last.to_i rescue 0
    end

    def behind?(threshold)
      seconds_behind > threshold
    end
  end
end

