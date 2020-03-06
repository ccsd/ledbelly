require 'hashdiff'
require 'jaro_winkler'

task :alter_tables do

  #DB_CFG = YAML.load_file('./cfg/database.yml')

  def sql_to_hash(log)
    out = {}
    current_event = ''
    File.read(log)&.each_line do |line|
      if event_match = line.match(/CREATE TABLE\s+(?:[^.]+\.)?((?:live|ims)_\w+)\s+\(/i)
        event_table_name_str = event_match.captures.first
        event_table_name = event_table_name_str.to_sym
        out[event_table_name] ||= {}
        current_event = event_table_name
      elsif column_name_match = line.match(/^\s{1}([a-z_]+)\s(.*)/i)
        event_column_name_str, event_column_name_type_str = column_name_match.captures
        out[current_event][event_column_name_str.to_sym] = event_column_name_type_str.chomp(',')
      end
    end
    out
  end

  def alter_tables_sql(diff, old_ddl, new_ddl)
    changes = {create: [], add: [], modify: [], rename: [], drop: [], double_check: []}
    renamed = []
    diff.each_with_index do |change, i|
      table_name, column_name = change[1].split('.')
      datatype = change.last

      case change.first
      # modify
      when '~'
        if ['tinytds', 'postgres'].include?(DB_CFG['adapter'])
          changes[:modify] << "ALTER TABLE #{table_name} ALTER COLUMN #{column_name} #{datatype};"
        elsif ['mysql2', 'oracle'].include?(DB_CFG['adapter'])
          changes[:modify] << "ALTER TABLE #{table_name} MODIFY COLUMN #{column_name} #{datatype};"
        end
      # remove
      when '-'
        # first, check if column was renamed
        next if diff[i+1].nil?
        next_change = diff[i+1]
        next_table_name, next_column_name = next_change[1].split('.')
        next_datatype = next_change.last
        
        # might be a column rename if...
        rename_if = {
          table_and_datatype_length: [table_name, datatype] == [next_table_name, next_datatype],
          table_and_datatype: [table_name, datatype.gsub(/(\W|\d)/, "")] == [next_table_name, next_datatype.gsub(/(\W|\d)/, "")],
          same_table_key_index: column_key_compare([table_name, column_name], [next_table_name, next_column_name], old_ddl, new_ddl),
          jw_distance: rename_weight(change, next_change)
        }.select { |k,v| v == true }
        # at least 2 of the 4
        if rename_if.size >= 2
          if ['tinytds'].include?(DB_CFG['adapter'])
            changes[:rename] << "EXEC sp_rename '#{table_name}.#{column_name}', '#{next_column_name}', 'COLUMN';"
          elsif ['mysql2'].include?(DB_CFG['adapter'])
            changes[:rename] << "ALTER TABLE #{table_name} CHANGE COLUMN `#{column_name}` `#{next_column_name}` #{next_datatype};"
          elsif ['oracle', 'postgres'].include?(DB_CFG['adapter'])
            changes[:rename] << "ALTER TABLE #{table_name} RENAME COLUMN #{column_name} TO #{next_column_name};"
          end
          renamed << "#{table_name}:::#{column_name}:::#{next_column_name}"
          changes[:double_check] << change
        # drop column
        else
          changes[:drop] << "ALTER TABLE #{table_name} DROP COLUMN #{column_name};"
        end
      # add
      when '+'
        # addition is a new event table
        if change.last.class == Hash
          changes[:create] << "-- ADD #{table_name} FROM CREATE TABLES"
        else
          next if diff[i-1].nil?
          # ensure this one wasn't setup as a rename
          prev_change = diff[i-1]
          prev_table_name, prev_column_name = prev_change[1].split('.')
          prev_datatype = prev_change.last
          # add if new, not if renamed
          if !renamed.any? { |r| r.include?("#{table_name}:::#{prev_column_name}:::#{column_name}") }
            changes[:add] << "ALTER TABLE #{table_name} ADD #{column_name} #{datatype};"
          else
            changes[:double_check] << change
          end
        end
      end
    end
    changes
  end

  def column_key_compare(change, next_change, old_ddl, new_ddl)
    # get the hash key index of the current column, in the old schema
    column_idx = old_ddl.dig(change[0].to_sym).find_index { |k,_| k== change[1].to_sym }
    # get the has key index of the next column, in the new schema
    next_column_idx = new_ddl.dig(next_change[0].to_sym).find_index { |k,_| k== next_change[1].to_sym }
    column_idx == next_column_idx
  end

  def rename_weight(change, next_change)
    (JaroWinkler.distance "#{change[1]} #{change.last}", "#{next_change[1]} #{next_change.last}", ignore_case: true) >= 0.91
  end

  def compare_sql(format)
    sql_out = []
    sql_files = Dir["sql/ddl/*#{format}*"].sort_by{ |f| File.birthtime(f) }[0...2]
    if sql_files.size < 2
      warn 'must have 2 to compare! run `rake create_tables`'
      exit
    end
    old_ddl = sql_to_hash(sql_files.first)
    new_ddl = sql_to_hash(sql_files.last)
    changes = Hashdiff.diff(old_ddl, new_ddl)
    sql_out <<   "--------------------"
    sql_out << "-- #{format} schema"
    sql_out << "-- comparing: " + sql_files.to_s
    sql_out <<   "--------------------"
    alter_tables_sql(changes, old_ddl, new_ddl).each do |type, changed|
      sql_out <<   "-- #{type}"
      changed.each { |change| sql_out <<  "   #{change}" }
      sql_out << ""
    end
    sql_out.join("\n")
  end

  schemas = Dir["sql/ddl/*sql"].map{ |s| File.basename(s).split(/[._\-]/)[0] }.uniq.sort
  schemas.each { |f| print compare_sql(f) + "\n" if ['canvas','caliper','custom'].include? f }
end