module IMSCaliperEvents

  # handle ims caliper
  def _imsdata(event_name, data)
    
    common = {
      uuid:                           data['id']&.to_s,
      action:                         data['action']&.to_s,
      actor_entity_id:                data['actor_entity_id']&.to_i,
      actor_id:                       data['actor_id']&.to_s,
      actor_real_user_id:             data['actor_real_user_id']&.to_i,
      actor_root_account_id:          data['actor_root_account_id']&.to_i,
      actor_root_account_lti_guid:    data['actor_root_account_lti_guid']&.to_s,
      actor_root_account_uuid:        data['actor_root_account_uuid']&.to_s,
      actor_type:                     data['actor_type']&.to_s,
      actor_user_login:               data['actor_user_login']&.to_s,
      actor_user_sis_id:              data['actor_user_sis_id']&.to_s,
      client_ip:                      data['client_ip']&.to_s,
      context:                        data['context']&.to_s,
      edapp_id:                       data['edapp_id']&.to_s,
      edapp_type:                     data['edapp_type']&.to_s,
      eventtime:                      data['eventtime'].nil? ? nil : default_timezone(data['eventtime']),
      group_context_type:             data['group_context_type']&.to_s,
      group_entity_id:                data['group_entity_id']&.to_i,
      group_id:                       data['group_id']&.to_s,
      group_type:                     data['group_type']&.to_s,
      hostname:                       data['hostname']&.to_s,
      job_id:                         data['job_id']&.to_i,
      job_tag:                        data['job_tag']&.to_s,
      membership_id:                  data['membership_id']&.to_s,
      membership_member_id:           data['membership_member_id']&.to_s,
      membership_member_type:         data['membership_member_type']&.to_s,
      membership_organization_id:     data['membership_organization_id']&.to_s,
      membership_organization_type:   data['membership_organization_type']&.to_s,
      membership_roles:               data['membership_roles']&.to_s,
      membership_type:                data['membership_type']&.to_s,
      object_id:                      data['object_id']&.to_s,
      object_entity_id:               data['object_entity_id']&.to_i,
      object_name:                    data['object_name']&.to_s,
      object_type:                    data['object_type']&.to_s,
      referrer:                       data['referrer']&.to_s,
      request_url:                    data['request_url']&.to_s,
      request_id:                     data['request_id']&.to_s,
      session_id:                     data['session_id']&.to_s,
      session_type:                   data['session_type']&.to_s,
      type:                           data['type']&.to_s,
      user_agent:                     data['user_agent']&.to_s,
      version:                        data['version']&.to_s,
    }.compact

    case event_name

    when 'asset_accessed'
      specific = {
        object_asset_name:          data['object_asset_name']&.to_s,
        object_asset_subtype:       data['object_asset_subtype']&.to_s,
        object_asset_type:          data['object_asset_type']&.to_s,
        object_context_account_id:  data['object_context_account_id']&.to_i,
        object_developer_key_id:    data['object_developer_key_id']&.to_i,
        object_display_name:        data['object_display_name']&.to_s,
        object_domain:              data['object_domain']&.to_s,
        object_filename:            data['object_filename']&.to_s,
        object_http_method:         data['object_http_method']&.to_s,
        object_url:                 data['object_url']&.to_s,
      }
    
    when 'assignment_created'
      specific = {
        object_datecreated:         data['object_datecreated'].nil? ? nil : default_timezone(data['object_datecreated']),
        object_maxscore:            data['object_maxscore']&.to_f,
        object_maxscore_numberstr:  data['object_maxscore_numberstr']&.to_f,
        object_description:         data['object_description']&.to_s,
        object_lock_at:             data['object_lock_at'].nil? ? nil : default_timezone(data['object_lock_at']),
        object_datetoshow:          data['object_datetoshow'].nil? ? nil : default_timezone(data['object_datetoshow']),
        object_datetosubmit:        data['object_datetosubmit'].nil? ? nil : default_timezone(data['object_datetosubmit']),
      }
    
    when 'assignment_updated'
      specific = {
        object_description:         data['object_description']&.to_s,
        object_datemodified:        data['object_datemodified'].nil? ? nil : default_timezone(data['object_datemodified']),
        object_workflow_state:      data['object_workflow_state']&.to_s,
        object_datetosubmit:        data['object_datetosubmit'].nil? ? nil : default_timezone(data['object_datetosubmit']),
        object_maxscore:            data['object_maxscore']&.to_f,
        object_maxscore_numberstr:  data['object_maxscore_numberstr']&.to_f,
        object_lock_at:             data['object_lock_at'].nil? ? nil : default_timezone(data['object_lock_at']),
        object_datetoshow:          data['object_datetoshow'].nil? ? nil : default_timezone(data['object_datetoshow']),
      }
    
    when 'assignment_override_created'
      specific = {
        object_assignment_id:      data['object_assignment_id']&.to_i,
        object_all_day_date:       data['object_all_day_date'].nil? ? nil : default_timezone(data['object_all_day_date']),
        object_all_day:            data['object_all_day']&.to_s,
        object_course_section_id:  data['object_course_section_id']&.to_s,
        object_datetoshow:         data['object_datetoshow'].nil? ? nil : default_timezone(data['object_datetoshow']),
        object_datetosubmit:       data['object_datetosubmit'].nil? ? nil : default_timezone(data['object_datetosubmit']),
        object_group_id:           data['object_group_id']&.to_i,
        object_lock_at:            data['object_lock_at'].nil? ? nil : default_timezone(data['object_lock_at']),
        object_workflow_state:     data['object_workflow_state']&.to_s,
        membership_organization_suborganizationof_id: data['membership_organization_suborganizationof_id']&.to_s,
        membership_organization_suborganizationof_type: data['membership_organization_suborganizationof_type']&.to_s,
      }
    
    when 'assignment_override_updated'
      specific = {
        object_assignment_id:      data['object_assignment_id']&.to_i,
        object_all_day_date:       data['object_all_day_date'].nil? ? nil : default_timezone(data['object_all_day_date']),
        object_all_day:            data['object_all_day']&.to_s,
        object_course_section_id:  data['object_course_section_id']&.to_s,
        object_datetoshow:         data['object_datetoshow'].nil? ? nil : default_timezone(data['object_datetoshow']),
        object_datetosubmit:       data['object_datetosubmit'].nil? ? nil : default_timezone(data['object_datetosubmit']),
        object_group_id:           data['object_group_id']&.to_i,
        object_lock_at:            data['object_lock_at'].nil? ? nil : default_timezone(data['object_lock_at']),
        object_workflow_state:     data['object_workflow_state']&.to_s,
        membership_organization_suborganizationof_id: data['membership_organization_suborganizationof_id']&.to_s,
        membership_organization_suborganizationof_type: data['membership_organization_suborganizationof_type']&.to_s,
      }

    when 'attachment_created'
      specific = {
        object_datecreated:   data['object_datecreated'].nil? ? nil : default_timezone(data['object_datecreated']),
        object_context_id:    data['object_context_id']&.to_i,
        object_context_type:  data['object_context_type']&.to_s,
        object_filename:      data['object_filename']&.to_s,
        object_folder_id:     data['object_folder_id']&.to_i,
        object_mediatype:     data['object_mediatype']&.to_s,
      }
    
    when 'attachment_deleted'
      specific = {
        object_datemodified:  data['object_datemodified'].nil? ? nil : default_timezone(data['object_datemodified']),
        object_context_id:    data['object_context_id']&.to_i,
        object_context_type:  data['object_context_type']&.to_s,
        object_filename:      data['object_filename']&.to_s,
        object_folder_id:     data['object_folder_id']&.to_i,
        object_mediatype:     data['object_mediatype']&.to_s,
      }
    
    when 'attachment_updated'
      specific = {
        object_datemodified:  data['object_datemodified'].nil? ? nil : default_timezone(data['object_datemodified']),
        object_context_id:    data['object_context_id']&.to_i,
        object_context_type:  data['object_context_type']&.to_s,
        object_filename:      data['object_filename']&.to_s,
        object_folder_id:     data['object_folder_id']&.to_i,
        object_mediatype:     data['object_mediatype']&.to_s,
      }

    when 'course_created'
      specific = {}
    
    when 'discussion_entry_created'
      specific = {
        object_ispartof_id:     data['object_ispartof_id']&.to_s,
        object_ispartof_type:   data['object_ispartof_type']&.to_s,
        object_body:            data['object_body']&.to_s,
      }
    
    when 'discussion_topic_created'
      specific = {
        object_is_announcement: data['object_is_announcement']&.to_s,
      }
    
    when 'enrollment_created'
      specific = {
        object_datecreated:                             data['object_datecreated'].nil? ? nil : default_timezone(data['object_datecreated']),
        object_course_id:                               data['object_course_id']&.to_s,
        object_course_section_id:                       data['object_course_section_id']&.to_s,
        object_limit_privileges_to_course_section:      data['object_limit_privileges_to_course_section']&.to_s,
        object_user_id:                                 data['object_user_id']&.to_s,
        object_user_name:                               data['object_user_name']&.to_s,
        object_workflow_state:                          data['object_workflow_state']&.to_s,
        membership_organization_suborganizationof_id:   data['membership_organization_suborganizationof_id']&.to_s,
        membership_organization_suborganizationof_type: data['membership_organization_suborganizationof_type']&.to_s,
      }
    
    when 'enrollment_state_created'
      specific = {
        object_access_is_current:   data['object_access_is_current']&.to_s,
        object_restricted_access:   data['object_restricted_access']&.to_s,
        object_state:               data['object_state']&.to_s,
        object_state_is_current:    data['object_state_is_current']&.to_s,
        object_state_valid_until:   data['object_state_valid_until']&.to_s,
        object_startedattime:       data['object_startedattime'].nil? ? nil : default_timezone(data['object_startedattime']),
      }
    
    when 'enrollment_state_updated'
      specific = {
        object_access_is_current:   data['object_access_is_current']&.to_s,
        object_restricted_access:   data['object_restricted_access']&.to_s,
        object_state:               data['object_state']&.to_s,
        object_state_is_current:    data['object_state_is_current']&.to_s,
        object_state_valid_until:   data['object_state_valid_until']&.to_s,
        object_startedattime:       data['object_startedattime'].nil? ? nil : default_timezone(data['object_startedattime']),
      }
    
    when 'enrollment_updated'
      specific = {
        object_datecreated:                               data['object_datecreated'].nil? ? nil : default_timezone(data['object_datecreated']),
        object_datemodified:                              data['object_datemodified'].nil? ? nil : default_timezone(data['object_datemodified']),
        object_course_id:                                 data['object_course_id']&.to_s,
        object_course_section_id:                         data['object_course_section_id']&.to_s,
        object_limit_privileges_to_course_section:        data['object_limit_privileges_to_course_section']&.to_s,
        object_user_id:                                   data['object_user_id']&.to_s,
        object_user_name:                                 data['object_user_name']&.to_s,
        object_workflow_state:                            data['object_workflow_state']&.to_s,
        membership_organization_suborganizationof_id:     data['membership_organization_suborganizationof_id']&.to_s,
        membership_organization_suborganizationof_type:   data['membership_organization_suborganizationof_type']&.to_s,
      }
    
    when 'grade_change'
      specific = {
        object_grade:                         data['object_grade']&.to_s,
        object_assignee_id:                   data['object_assignee_id']&.to_s,
        object_assignee_type:                 data['object_assignee_type']&.to_s,
        object_assignee_sis_id:               data['object_assignee_sis_id']&.to_s,
        object_assignable_id:                 data['object_assignable_id']&.to_s,
        object_assignable_type:               data['object_assignable_type']&.to_s,
        generated_id:                         data['generated_id']&.to_s,
        generated_type:                       data['generated_type']&.to_s,
        generated_grade:                      data['generated_grade']&.to_s,
        generated_entity_id:                  data['generated_entity_id']&.to_s,
        generated_attempt_id:                 data['generated_attempt_id']&.to_s,
        generated_attempt_type:               data['generated_attempt_type']&.to_s,
        generated_attempt_grade:              data['generated_attempt_grade']&.to_s,
        generated_attempt_assignee_id:        data['generated_attempt_assignee_id']&.to_s,
        generated_attempt_assignee_type:      data['generated_attempt_assignee_type']&.to_s,
        generated_attempt_assignee_sis_id:    data['generated_attempt_assignee_sis_id']&.to_s,
        generated_attempt_assignable_id:      data['generated_attempt_assignable_id']&.to_s,
        generated_attempt_assignable_type:    data['generated_attempt_assignable_type']&.to_s,
        generated_maxscore_numberstr:         data['generated_maxscore_numberstr']&.to_f,
        generated_scoregiven_numberstr:       data['generated_scoregiven_numberstr']&.to_f,
        generated_scoredby:                   data['generated_scoredby']&.to_s,
        generated_scoregiven:                 data['generated_scoregiven']&.to_f,
        generated_maxscore:                   data['generated_maxscore']&.to_f,
      }
    
    when 'group_created'
      specific = {
        object_ispartof_id:                   data['object_ispartof_id']&.to_s,
        object_ispartof_name:                 data['object_ispartof_name']&.to_s,
        object_ispartof_type:                 data['object_ispartof_type']&.to_s,
      }

    when 'group_category_created'
      specific = {}
    
    when 'group_membership_created'
      specific = {
        object_member_id:                   data['object_member_id']&.to_s,
        object_member_type:                 data['object_member_type']&.to_s,
        object_organization_entity_id:      data['object_organization_entity_id']&.to_s,
        object_organization_id:             data['object_organization_id']&.to_s,
        object_organization_ispartof_id:    data['object_organization_ispartof_id']&.to_s,
        object_organization_ispartof_name:  data['object_organization_ispartof_name']&.to_s,
        object_organization_ispartof_type:  data['object_organization_ispartof_type']&.to_s,
        object_organization_name:           data['object_organization_name']&.to_s,
        object_organization_type:           data['object_organization_type']&.to_s,
        object_roles:                       data['object_roles']&.to_s,
      }

    when 'logged_in'
      specific = {
        object_redirect_url:    data['object_redirect_url']&.to_s,
      }
    
    when 'logged_out'
      specific = {}
    
    when 'quiz_submitted'
      specific = {
        object_assignee_id:       data['object_assignee_id']&.to_s,
        object_assignee_type:     data['object_assignee_type']&.to_s,
        object_assignable_id:     data['object_assignable_id']&.to_s,
        object_assignable_type:   data['object_assignable_type']&.to_s,
      }
    
    when 'submission_created'
      specific = {
        object_datecreated:       data['object_datecreated'].nil? ? nil : default_timezone(data['object_datecreated']),
        object_submission_type:   data['object_submission_type']&.to_s,
        object_assignee_id:       data['object_assignee_id']&.to_s,
        object_assignee_type:     data['object_assignee_type']&.to_s,
        object_assignable_id:     data['object_assignable_id']&.to_s,
        object_assignable_type:   data['object_assignable_type']&.to_s,
        object_count:             data['object_count']&.to_s,
        object_body:              data['object_body']&.to_s,
        object_url:               data['object_url']&.to_s,
    }
    
    when 'submission_updated'
      specific = {
        object_datemodified:    data['object_datemodified'].nil? ? nil : default_timezone(data['object_datemodified']),
        object_assignee_id:     data['object_assignee_id']&.to_s,
        object_assignee_type:   data['object_assignee_type']&.to_s,
        object_assignable_id:   data['object_assignable_id']&.to_s,
        object_assignable_type: data['object_assignable_type']&.to_s,
        object_submission_type: data['object_submission_type']&.to_s,
        object_count:           data['object_count']&.to_s,
        object_url:             data['object_url']&.to_s,
        object_body:            data['object_body']&.to_s,
      }
    
    when 'syllabus_updated'
      specific = {
        object_creators_id:     data['object_creators_id']&.to_s,
        object_creators_type:   data['object_creators_type']&.to_s,
        object_body:            data['object_body']&.to_s,
      }

    when 'user_account_association_created'
      specific = {
        object_datecreated:   data['object_datecreated'].nil? ? nil : default_timezone(data['object_datecreated']),
        object_datemodified:  data['object_datemodified'].nil? ? nil : default_timezone(data['object_datemodified']),
        object_is_admin:      data['object_is_admin']&.to_s,
        object_user_id:       data['object_user_id']&.to_s,
      }
      
    when 'wiki_page_created'
      specific = {
          object_body:        data['object_body']&.to_s,
      }
    
    when 'wiki_page_deleted'
      specific = {}
    
    when 'wiki_page_updated'
      specific = {
          object_body:        data['object_body']&.to_s,
      }

    # catch and save events, we don't have configured or we aren't expecting
    else
      collect_unknown(event_name, event_data)
      # return if the message cannot be prepped for import
      return
    end

    # return parsed event data
    # merge the common fields with the event specific data
    specific.merge(common)
  end

  def _caliper(event_name, event_data)
    data = _squish(event_data['data'][0])
    imsdata = _imsdata(event_name, data)
    return if !imsdata.is_a? Hash

    # check if we missed any new data
    caliper_count(event_name, data, imsdata)

    # return event data - parsed, flattened, ready for sql
    imsdata
  end
end