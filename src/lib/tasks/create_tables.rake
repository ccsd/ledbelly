require 'yaml'

task :create_tables do

  DB_CFG = YAML.load_file('./cfg/database.yml')
  adapter = DB_CFG['adapter']
  # the name of the schema/database name... might be 'dbo' for tinytds or whatever you named the db
  dbname = DB_CFG['data']

  primary_keys = {
    mysql2:   'BIGINT NOT NULL AUTO_INCREMENT',
    oracle:   'NUMBER GENERATED ALWAYS AS IDENTITY',
    postgres: 'SERIAL PRIMARY KEY',
    tinytds:  'BIGINT IDENTITY(1,1) PRIMARY KEY'
  }

  Dir.glob('./src/lib/schemas/*.rb') do |schema_hash|
    require schema_hash
    format = File.basename(schema_hash, ".rb")
    ddlout = []

    # tables
    $schema.each do |table, columns|
      # create table
      tbobj = [ 'mysql2', 'tinytds' ].include?(adapter) ? "#{dbname}.#{table}" : table
      if adapter == 'tinytds'
        ddlout << "IF OBJECT_ID('#{dbname}.#{table}', 'U') IS NOT NULL DROP TABLE #{dbname}.#{table};\n"
        ddlout << "IF NOT EXISTS (SELECT * FROM sysobjects WHERE name = '#{table}' AND xtype = 'U')\n"
      else
        ddlout << "DROP TABLE IF EXISTS #{tbobj};\n"
      end
      ddlout << "CREATE TABLE #{tbobj} (\n"

      # columns
      colout = []
      pk_col = ''
      columns.each do |column, params|
        # primary key
        if params[:primary_key] == true 
          colout << "\t#{column} #{primary_keys[adapter.to_sym]}"
          pk_col = column
        # others
        else

          # handle type swapping
          case params[:type]
          when 'string'
            if params[:size] == 'MAX' && adapter != 'tinytds'
              mysql_utf8 = adapter == 'mysql2' ? ' CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci' : nil
              colout << "\t#{column} TEXT#{mysql_utf8}" 
            else
              coltype = [ 'tinytds' ].include?(adapter) && params[:mbstr] == true ? 'NVARCHAR' : 'VARCHAR'
              colout << "\t#{column} #{coltype}(#{params[:size]})"
            end
          when 'datetime'
            dtype = [ 'mysql2', 'tinytds' ].include?(adapter) ? 'DATETIME' : 'TIMESTAMP'
            colout << "\t#{column} #{dtype}"
          
          # no specific swapping
          else
            coltype = adapter != 'oracle' ? params[:type] : 'NUMBER'
            # size is set
            colsize = !params[:size].nil? ? "\t#{column} #{coltype}(#{params[:size]})" : "\t#{column} #{coltype.upcase}"
            colout << colsize
          end
        end
      end
      if adapter == 'mysql2'
        colout << "\tPRIMARY KEY (`#{pk_col}`)"
      end
      ddlout << colout.join(",\n")
      close_table = adapter == 'mysql2' ? ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;" : ");"
      ddlout << close_table
    end

    # puts ddlout
    open("./sql/ddl/#{format}-create-#{adapter}-#{Time.now.strftime('%Y%m%d%H%M%S')}.sql", 'w') { |f| f.puts(ddlout) }
  end
end