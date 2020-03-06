require 'shoryuken'
require 'sequel'
require 'logger'
require_relative 'src/ledbelly_settings'
require_relative 'src/ledbelly_support'
require_relative 'src/ledbelly_worker'

# load sub services
LED['services']&.each do |service|
  require_relative "src/lib/services/#{service}.rb"
end