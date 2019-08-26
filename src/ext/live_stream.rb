module LiveStream

  def live_stream(event_name, event_time, event_data)

    meta = event_data['metadata']
    body = event_data['body']

    data = {
      # stream data
      event_name:             meta['event_name']&.to_s,
      event_time:             meta['event_time'].nil? ? nil : default_timezone(meta['event_time']),
      real_user_id:           meta['real_user_id']&.to_i,
      user_id_body:           body['user_id']&.to_i,
      user_id_meta:           meta['user_id']&.to_i,
      user_login:             meta['user_login']&.to_s,
      user_sis_id:            meta['user_sis_id']&.to_s,
      user_account_id:        meta['user_account_id']&.to_i,
      user_agent:             meta['user_agent']&.to_s,
      context_id_body:        body['context_id']&.to_i,
      context_id_meta:        meta['context_id']&.to_i,
      context_role_body:      body['context_role']&.to_s,
      context_role_meta:      meta['context_role']&.to_s,
      context_type_body:      body['context_type']&.to_s,
      context_type_meta:      meta['context_type']&.to_s,
      context_sis_source_id:  meta['context_sis_source_id']&.to_s,
      # request
      client_ip:              meta['client_ip']&.to_s,
      time_zone:              meta['time_zone']&.to_s,
      request_id:             meta['request_id']&.to_s,
      session_id:             meta['session_id']&.to_s,
      url_meta:               meta['url']&.to_s,
      http_method:            meta['http_method']&.to_s,
      developer_key_id:       meta['developer_key_id']&.to_i,
      # assets
      asset_id:               body['asset_id']&.to_i,
      asset_type:             body['asset_type']&.to_s,
      asset_subtype:          body['asset_subtype']&.to_s,
      asset_category:         body['category']&.to_s,
      asset_role:             body['role']&.to_s,
      asset_level:            body['level']&.to_s,
      # submissions
      submission_id:          body['submission_id']&.to_i,
      assignment_id:          body['assignment_id']&.to_i,
      quiz_id:                body['quiz_id']&.to_i,
      submitted_at:           body['submitted_at'].nil? ? nil : default_timezone(body['submitted_at']),
      # system meta
      hostname:               meta['hostname']&.to_s,
      job_id:                 meta['job_id']&.to_i,
      job_tag:                meta['job_tag']&.to_s,
      producer:               meta['producer']&.to_s,
      root_account_id:        meta['root_account_id']&.to_i,
      root_account_lti_guid:  meta['root_account_lti_guid']&.to_s,
      root_account_uuid:      meta['root_account_uuid']&.to_s,
      # generic
      workflow_state:         body['workflow_state']&.to_s,
    }.compact

    # passively truncate strings to DDL length, keeps data insertion, logs warning for manual update
    limit_to_ddl('live_stream', data)

    processed = Time.new
    created = {
      processed_at:     processed.strftime('%Y-%m-%d %H:%M:%S.%L').to_s,
      event_time_local: Time.parse(event_time).utc.localtime.strftime(TIME_FORMAT).to_s,
    }
    data = data.merge(created)
    
    begin
      DB[:live_stream].insert(data)
    rescue => e
      handle_db_errors(e, event_name, event_data, data)
    end
  end
end