require_relative 'events/canvas_raw'
require_relative 'events/ims_caliper'
require_relative 'ext/live_stream'
require_relative 'actions/import_sql'

class LiveEvents
  include Shoryuken::Worker
  include CanvasRawEvents
  include IMSCaliperEvents
  include SQLInsertEvent
  include LiveStream

  shoryuken_options queue: SQS_CFG['queues'][0], auto_delete: true

  # terminal output, if terminal/interactive
  puts 'LEDbelly loaded, consuming events...' if $stdout.isatty

  # https://github.com/phstc/shoryuken/wiki/Worker-options#body_parser
  shoryuken_options body_parser: :json
  shoryuken_options body_parser: ->(sqs_msg){ REXML::Document.new(sqs_msg.body) }
  shoryuken_options body_parser: JSON

  # https://github.com/phstc/shoryuken/blob/master/lib/shoryuken/worker/inline_executor.rb#L8
  def perform(sqs_msg, _body)

    # event attributes
    event_name = sqs_msg.message_attributes.dig('event_name', 'string_value')
    event_time = sqs_msg.message_attributes.dig('event_time', 'string_value')
    event_data = JSON.parse(sqs_msg.body)

    begin
      # handle caliper
      if event_data['dataVersion'] == 'http://purl.imsglobal.org/ctx/caliper/v1p1'
                
        # pass to parser
        event_parsed = _caliper(event_name, event_data)
        # import to db
        import(event_name, event_time, event_data, event_parsed)

      # handle canvas raw
      else

        # parse event data
        event_parsed = _canvas(event_data)
        # import to db
        import(event_name, event_time, event_data, event_parsed)

        # extras
        live_stream(event_name, event_time, event_data)
      end
    rescue => e
      warn "#{event_name} #{default_timezone(event_time)}\n#{e}"
      warn e.backtrace
      raise
    end
  end

end