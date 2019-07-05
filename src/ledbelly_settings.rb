# LEDbelly config
LED = YAML.load_file('./cfg/ledbelly.yml')

# shoryuken config and settings
SQS_CFG = YAML.load_file('./cfg/sqs.yml')
Shoryuken.sqs_client = Aws::SQS::Client.new(
  region: SQS_CFG['aws']['region'],
  credentials: Aws::Credentials.new(
    SQS_CFG['aws']['access_key_id'],
    SQS_CFG['aws']['secret_access_key']
  )
)
# https://github.com/phstc/shoryuken/wiki/Long-Polling
Shoryuken.sqs_client_receive_message_opts = {
  wait_time_seconds: 20
}

# database config and settings
DB_CFG = YAML.load_file('./cfg/database.yml')
# db connection parameters
DB = Sequel.connect(
  adapter: DB_CFG['adapter'],
  host: DB_CFG['host'],
  database: DB_CFG['data'],
  user: DB_CFG['user'],
  password: DB_CFG['pass'],
  max_connections: DB_CFG['max_connections'],
  encoding: 'utf8', # mssql
  # charset: 'utf8mb4', # mysql
  timeout: 180,
  pool_timeout: 45,
  # this will log every single transactions statement, results in very large files
  # logger: Logger.new('log/database.log', 'daily')
)
# Sequel error extension 
DB.extension :error_sql
# convert table and column names to downcase
DB.extension :identifier_mangling
DB.identifier_input_method = :downcase
if DB_CFG['adapter'] == 'tinytds'
  DB.run("SET ANSI_WARNINGS ON;")
end

# collects everythig in lib/schemas and compiles it into 1 large hash
ddl_stack = {}
Dir.glob('./src/lib/schemas/*.rb') do |schema|
  require schema
  ddl_stack = ddl_stack.merge!($schema)
end
# then stored as a constant
EVENT_DDL = ddl_stack.freeze

# format for all time strings
TIME_FORMAT = '%Y-%m-%d %H:%M:%S'.freeze

# broad warnings for available adapters
WARN_ERRORS = [
  # mysql
  # oracle
  'table or view does not exist', # missing table
  'invalid identifier', # missing column
  # postgre
  # 'relation "afd" does not exist' # relation "" does not exist
  # 'column "sdf" does not exist' # column "" does not exist
  # tinytds
  'Invalid object name', # missing table
  'Invalid column name', # missing column
  'String or binary data would be truncated', # value larger than column definition
].freeze

# broad errors worthy of disconnecting, to preserve messages in the queue
DISCONNECT_ERRORS = [
  # mysql
  # oracle
  # postgre
  # tinytds
  'Adaptive Server connection timed out',
  'Cannot continue the execution because the session is in the kill state',
  'Login failed for user',
  'Read from the server failed',
  'Server name not found in configuration files',
  'The transaction log for database',
  'Unable to access availability database',
  'Unable to connect: Adaptive Server is unavailable or does not exist',
  'Write to the server failed',
].freeze