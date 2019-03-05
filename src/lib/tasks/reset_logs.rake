task :reset_logs do
  Dir.glob('./log/*.log') do |logfile|
    begin
      open(logfile, 'w') { |f| f.puts(nil) }
      puts logfile
    rescue => e
      puts e
    end
  end
end