# reduces log files to a somewhat intelligent guess of what needs to be updated
# prevents reviewing thousands of log file lines, trying to keep schema's updated

task :reduce_logs do

  def open_log(log)
    if File.exist?(log)
      File.read(log)
    end
  end

  # reduces log/ddl-warnings.log, indentifying unique updates and tries to provide sample datatype from schema
  def ddl_warnings
    updates = []
    open_log("log/ddl-warnings.log")&.each_line do |line|
      if match = line.match(/(live_[a-z_]+).([a-z_]+)\s--\s(.*)/i)
        event_name, column, value = match.captures
        sample = warnings_type_test(column)
        sample = sample.size == 1 ? sample.first.strip : 'too many choices'
        updates << "#{event_name} | #{column} | #{sample}"
      end
    end
    puts "### updates for schema column length"
    updates.uniq.sort.each { |line| puts line }
  end

  def warnings_type_test(check)
    schema = `git show HEAD:src/schemas/canvas.rb`
    size = schema.each_line.select { |line| line =~ /#{check}/ }
    size.uniq
  end
  
  # reduces log/ddl-undefined.log, identifying unique columns that need to be added to schema
  def undefined_columns
    updates = {}
    open_log("log/ddl-undefined.log")&.each_line do |line|
      if match = line.match(/sample: { event_name: ([a-z_.]+), undefined: \[(.*)\]/i)
        event_name_str, undefined = match.captures
        event_name = event_name_str.to_sym
        updates[event_name] ||= {}
        #sample = undefined.delete('"').delete(' ').split(',')
        sample = undefined[1..-1].split('", "') 
        sample.each do |s|
          column_str, value = s.strip.split(':::')
          column = column_str.to_sym
          key = updates.dig(event_name, column)

          # check if current value is larger than stored value, keep largest/length
          updates[event_name][column] = if !!key && key[:value]&.size.to_i > value&.size.to_i
            { value: key[:value], type: key[:value]&.class, size: key[:size] }
          else
            { value: value, type: undefined_type_test?(value), size: value&.size.to_i }
          end
        end
      end
    end
    puts "\n\n### updates for events, columns not defined"
    updates.each do |event_name, columns|
      puts "#{event_name}"
      columns.each do |column, params|
        puts "    #{column}: { type: '#{params[:type]}', size: #{params[:size]} } # #{params[:value]}"
      end
    end
  end

  def undefined_type_test? string
    'Integer' if Integer(string) rescue 'Float' if Float(string) rescue 'String'
  end

  # reduces log/sql-truncations.log, identifing what event columns need to be updated with longer lengths
  def sql_truncations
    updates = {}
    open_log("log/sql-truncations.log")&.each_line do |line|
      if match = line.match(/([a-z_]+).([a-z_]+) {\ssupplied: (\d+), expecting: (\d+) }/i)
        event_name_str, column_str, supplied, expected = match.captures
        column = column_str.to_sym
        event_name = event_name_str.to_sym
        updates[event_name] ||= {}
        key = updates.dig(event_name, column)

        # check if current value is larger than stored value, keep largest/length
        updates[event_name][column] = if !!key && key[:supplied]&.to_i > supplied&.to_i
          { supplied: key[:supplied], expected: key[:expected] }
        else
          { supplied: supplied, expected: expected }
        end
      end
    end
    puts "\n\n### sql data truncated on insert, update column length"
    updates.each do |event_name, columns|
      puts "#{event_name}"
      columns.each do |column, params|
        puts "    #{column}: { supplied: '#{params[:supplied]}', expecting: '#{params[:expected]}' }"
      end
    end
  end
  
  ddl_warnings
  undefined_columns
  sql_truncations
end