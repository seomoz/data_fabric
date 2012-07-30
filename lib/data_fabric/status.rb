require 'singleton'

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
