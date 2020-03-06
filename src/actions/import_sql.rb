module SQLInsertEvent
  
  def import(event_name, event_time, event_data, import_data)

    event_source = event_data.dig('dataVersion').nil? ? 'live' : 'ims'
    event_table = "#{event_source}_#{event_name}".gsub(/[^\w\s]/, '_')

    # passively truncate strings to DDL length, keeps data insertion, logs warning for manual update
    limit_to_ddl(event_table, import_data)

    processed = Time.new
    created = {
      processed_at:     processed.strftime('%Y-%m-%d %H:%M:%S.%L').to_s,
      event_time_local: Time.parse(event_time).utc.localtime.strftime(TIME_FORMAT).to_s,
    }
    data = import_data.merge(created)

    begin
      # insert the data
      DB[event_table.to_sym].insert(data)

      # terminal output, if terminal/interactive
      printf("\r%s: %s\e[0J", created[:event_time_local], event_table) if $stdout.isatty
    rescue => e
      handle_db_errors(e, event_name, event_data, import_data)
    end
  end

  def limit_to_ddl(event_table, import_data)
    # loop through each string value, compare length to defined (DDL) length
    # if defined as multibyte string, check values bytesize against mb/length (defined 2 x actual byte length)
    # https://api.rubyonrails.org/classes/ActiveSupport/Multibyte/Chars.html#method-i-limit
    import_data.each do |k,v|
      next if v.nil?

      # log warnings if the key is not defined in the schema
      unless EVENT_DDL[event_table.to_sym].key?(k)
        log = "\nunexpected key, not found in schema : #{event_table}.#{k} -- #{v}"
        open('log/ddl-warnings.log', 'a') { |f| f << "#{Time.now} #{log}\n" }
        # remove the key from the hash
        import_data.delete(k)
        next
      end

      next unless EVENT_DDL[event_table.to_sym][k][:type] == 'string' && EVENT_DDL[event_table.to_sym][k][:size] != 'MAX'

      v = if EVENT_DDL[event_table.to_sym][k].key?(:mbstr)
        # multi-byte strings
        v.mb_chars.limit(EVENT_DDL[event_table.to_sym][k][:size] * 2).to_s  
      else
        begin
          # regular string
          v.mb_chars.limit(EVENT_DDL[event_table.to_sym][k][:size]).to_s
        rescue => e
          puts EVENT_DDL[event_table.to_sym][k]
          puts e
          puts "########{event_table}"
        end
      end

      next unless import_data[k].mb_chars.length > v.mb_chars.length

      # collect warning
      log = "#{event_table}.#{k} { supplied: #{import_data[k].mb_chars.length}, expecting: #{EVENT_DDL[event_table.to_sym][k][:size]} }"
      # overwrite/update inserted value
      import_data[k] = v
      # log warning
      open('log/sql-truncations.log', 'a') { |f| f << "#{Time.now} #{log}\n" }
    end
  end

  def handle_db_errors(exp, event_name, event_data, import_data) 

    # create a log entry
    err = %W[
      ---#{event_name}---\n
      #{exp.message}\n
      ------\n
      #{exp.sql}\n
      ------ import data\n
      #{import_data}\n
      ------ event data\n
      #{event_data}\n
    ].join
    # puts err
    
    # store in log file
    open('log/sql-errors.log', 'a') { |f| f << "#{err}\n\n" }
    # drop the failed SQL statement into a file
    # we can use this file to import the records later
    open('log/sql-recovery.log', 'a') { |f| f << "#{exp.sql};\n" }

    if exp.message.match? Regexp.union(WARN_ERRORS)
      warn "#{exp.message} (#{event_name})"
    end

    if exp.message.match? Regexp.union(DISCONNECT_ERRORS)
      # disconnect the db
      DB.disconnect

      # terminal output, if terminal/interactive
      warn exp.message if $stdout.isatty

      # kill shoryuken/ledbelly
      shoryuken_pid = File.read('log/shoryuken.pid').to_i
      Process.kill('TERM', shoryuken_pid)
    end
  end

end