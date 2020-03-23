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
  encoding: 'utf8', # encoding works for tinytds, mysql2, and postgres adapters (whereas charset is mysql specific)
  # tinytds = 'utf8', mysql = 'utf8mb4', oracle = 'AL32UTF8', postgres = 'UTF8'
  timeout: 600,
  pool_timeout: 45,
  # this will log every single transactions statement, results in very large files
  # logger: Logger.new('log/database.log', 'daily')
)
# The connection_validator extension modifies a database's
# connection pool to validate that connections checked out
# from the pool are still valid, before yielding them for use
DB.extension :connection_validator
# DB.pool.connection_validation_timeout = DB_CFG['timeout']

# Sequel error extension 
DB.extension :error_sql
# convert table and column names to downcase
DB.extension :identifier_mangling
DB.identifier_input_method = :downcase
# https://github.com/jeremyevans/sequel/blob/master/doc/opening_databases.rdoc#tinytds
if DB_CFG['adapter'] == 'tinytds'
  DB.run("SET ANSI_WARNINGS ON;")
end

# collects everythig in lib/schemas and compiles it into 1 large hash
ddl_stack = {}
Dir["./src/schemas/*.rb"].sort.each do |schema|
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
  # postgres
  'UndefinedColumn',
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
  'MySQL server has gone away',
  'Lost connection to MySQL server during query',
  # oracle
  # postgres
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
  'Cannot open user default database. Login failed.'
].freeze

# broad errors worthy of disconnecting, to preserve messages in the queue
IDLE_ERRORS = [
  # tinytds
  # 'is being recovered. Waiting until recovery is finished',
  # 'because the database replica is not in the PRIMARY or SECONDARY role',
  # 'is participating in an availability group and is currently not accessible for queries'
].freeze
