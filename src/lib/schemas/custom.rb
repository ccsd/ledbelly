$schema = {

  live_stream: {
    # created
    live_stream_id: { type: 'bigint', primary_key: true },
    processed_at: { type: 'datetime' },
    event_time_local: { type: 'datetime' },
    # stream data
    event_name: { type: 'string', size: 64 },
    event_time: { type: 'datetime' },
    request_id: { type: 'string', size: 36 },
    session_id: { type: 'string', size: 32 },
    client_ip: { type: 'string', size: 39 },
    real_user_id: { type: 'bigint' },
    user_id_body: { type: 'bigint' },
    user_id_meta: { type: 'bigint' },
    user_login: { type: 'string', size: 64 },
    user_sis_id: { type: 'string', size: 32 },
    user_account_id: { type: 'bigint' },
    user_agent: { type: 'string', size: 255 },
    context_id_body: { type: 'bigint' },
    context_id_meta: { type: 'bigint' },
    context_role_body: { type: 'string', size: 24 },
    context_role_meta: { type: 'string', size: 24 },
    context_type_body: { type: 'string', size: 24 },
    context_type_meta: { type: 'string', size: 24 },  
    context_sis_source_id: { type: 'string', size: 32 },
    # asset
    asset_id: { type: 'bigint' },
    asset_type: { type: 'string', size: 24 },
    asset_subtype: { type: 'string', size: 24 },
    asset_category: { type: 'string', size: 24 },
    asset_level: { type: 'string', size: 24 },
    asset_role: { type: 'string', size: 24 },
    # submissions
    submission_id: { type: 'bigint' },
    assignment_id: { type: 'bigint' },
    quiz_id: { type: 'bigint' },
    submitted_at: { type: 'datetime' },
    # system meta
    hostname: { type: 'string', size: 64 },
    producer: { type: 'string', size: 12 },
    root_account_id: { type: 'bigint' },
    root_account_lti_guid: { type: 'string', size: 40 },
    root_account_uuid: { type: 'string', size: 40 },
    job_id: { type: 'bigint' },
    job_tag: { type: 'string', size: 100 },
  }
}