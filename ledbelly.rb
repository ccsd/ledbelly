require 'shoryuken'
require 'sequel'
require 'logger'
require_relative 'src/ledbelly_settings'
require_relative 'src/ledbelly_worker'

# terminal output, if terminal/interactive
puts 'LEDbelly loaded, consuming events...' if $stdout.isatty