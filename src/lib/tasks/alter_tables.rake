require 'hashdiff'

task :alter_tables do

  DB_CFG = YAML.load_file('./cfg/database.yml')

  def sql_to_hash(log)
    out = {}
    current_event = ''
    File.read(log)&.each_line do |line|
      if event_match = line.match(/CREATE TABLE[^.]+\.((?:live|ims)_\w+) \(/i)
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

  def alter_tables_sql(diff)
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
        if [table_name, datatype] == [next_table_name, next_datatype] && column_name != next_column_name
          if ['tinytds'].include?(DB_CFG['adapter'])
            changes[:rename] << "EXEC sp_rename '#{table_name}.#{column_name}', '#{next_column_name}', 'COLUMN';"
          elsif ['mysql2'].include?(DB_CFG['adapter'])
            changes[:rename] << "ALTER TABLE #{table_name} CHANGE COLUMN #{column_name} TO #{next_column_name};"
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

  def compare_sql(format)
    sql_out = []
    compare_sql = Dir["sql/ddl/*#{format}*"].sort_by{ |f| File.mtime(f) }[0...2].reverse
    new_sql = sql_to_hash(compare_sql.first)
    old_sql = sql_to_hash(compare_sql.last)
    changes = Hashdiff.diff(new_sql, old_sql)
    sql_out << "-- #{format} schema\n"
    sql_out <<   "--------------------"
    alter_tables_sql(changes).each do |type, changed|
      sql_out <<   "-- #{type}"
      changed.each { |change| sql_out <<  "   #{change}" }
      sql_out << ""
    end
    sql_out.join("\n")
  end

  schemas = Dir["sql/ddl/*sql"].map{ |s| File.basename(s).split(/[._\-]/)[0] }.uniq.sort
  schemas.each { |f| print compare_sql(f) if ['canvas','caliper'].include? f }
end