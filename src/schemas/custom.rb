$schema = {

  live_stream: {
    # created
    live_stream_id: { type: 'bigint', primary_key: true },
    processed_at: { type: 'datetime' },
    event_time_local: { type: 'datetime' },
    # stream data
    event_name: { type: 'string', size: 64 },
    event_time: { type: 'datetime' },
    real_user_id: { type: 'bigint' },
    user_id_body: { type: 'bigint' },
    user_id_meta: { type: 'bigint' },
    user_login_meta: { type: 'string', size: 64 },
    user_sis_id_meta: { type: 'string', size: 32 },
    user_account_id: { type: 'bigint' },
    user_agent: { type: 'string', size: 512 },
    context_id_body: { type: 'bigint' },
    context_id_meta: { type: 'bigint' },
    context_role_body: { type: 'string', size: 24 },
    context_role_meta: { type: 'string', size: 24 },
    context_type_body: { type: 'string', size: 24 },
    context_type_meta: { type: 'string', size: 24 },  
    context_sis_source_id: { type: 'string', size: 32 },
    # request
    client_ip: { type: 'string', size: 39 },
    time_zone: { type: 'string', size: 255 },
    request_id: { type: 'string', size: 36 },
    session_id: { type: 'string', size: 32 },
    url_meta: { type: 'string', size: 'MAX' },
    referrer: { type: 'string', size: 'MAX' },
    http_method: { type: 'string', size: 7 },
    developer_key_id: { type: 'bigint' },
    # assets
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
    root_account_lti_guid: { type: 'string', size: 100 },
    root_account_uuid: { type: 'string', size: 40 },
    job_id: { type: 'bigint' },
    job_tag: { type: 'string', size: 100 },
    # generic
    workflow_state: { type: 'string', size: 256 },
  },

  led_geo_maxmind: {
    id: { type: 'bigint', primary_key: true },
    ip: { type: 'string', size: 39 },
    city: { type: 'string', size: 255 },
    region_name: { type: 'string', size: 255 },
    region_code: { type: 'string', size: 255 },
    country_code: { type: 'string', size: 6 },
    country_name: { type: 'string', size: 84 },
    continent_code: { type: 'string', size: 12 },
    in_eu: { type: 'string', size: 5 },
    postal: { type: 'string', size: 12 },
    latitude: { type: 'real', size: 4 },
    longitude: { type: 'real', size: 4 },
    timezone: { type: 'string', size: 50 },
    utc_offset: { type: 'string', size: 6 },
    country_calling_code: { type: 'string', size: 6 },
    currency: { type: 'string', size: 6 },
    languages: { type: 'string', size: 100 },
    asn: { type: 'string', size: 32 },
    org: { type: 'string', size: 255 },
  }
}