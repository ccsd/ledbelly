require 'shoryuken'
require 'sequel'
require 'logger'
require './src/ledbelly_settings'
require './src/ledbelly_worker'

# terminal output, if terminal/interactive
if $stdout.isatty
  puts 'LEDbelly loaded, consuming events...'
end