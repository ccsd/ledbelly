
module CanvasRawEvents

  # collect and return all possible message data fields
  def _metadata(event_data)
    meta = event_data['metadata']
    {
      client_ip:              meta['client_ip']&.to_s,
      context_account_id:     meta['context_account_id']&.to_i,
      context_id_meta:        meta['context_id']&.to_i,
      context_role_meta:      meta['context_role']&.to_s,
      context_sis_source_id:  meta['context_sis_source_id']&.to_s,
      context_type_meta:      meta['context_type']&.to_s,
      event_name:             meta['event_name']&.to_s,
      event_time:             meta['event_time'].nil? ? nil : default_timezone(meta['event_time']),
      developer_key_id:       meta['developer_key_id']&.to_i,
      hostname:               meta['hostname']&.to_s,
      http_method:            meta['http_method']&.to_s,
      job_id:                 meta['job_id']&.to_i,
      job_tag:                meta['job_tag']&.to_s,
      producer:               meta['producer']&.to_s,
      real_user_id:           meta['real_user_id']&.to_i,
      request_id:             meta['request_id']&.to_s,
      root_account_id_meta:   meta['root_account_id']&.to_i,
      root_account_lti_guid:  meta['root_account_lti_guid']&.to_s,
      root_account_uuid:      meta['root_account_uuid']&.to_s,
      session_id:             meta['session_id']&.to_s,
      time_zone:              meta['time_zone']&.to_s,
      url_meta:               meta['url']&.to_s,
      referrer:               meta['referrer']&.to_s,
      user_account_id:        meta['user_account_id']&.to_i,
      user_agent:             meta['user_agent']&.to_s,
      user_id_meta:           meta['user_id']&.to_i,
      user_login_meta:        meta['user_login']&.to_s,
      user_sis_id_meta:       meta['user_sis_id']&.to_s,
    }.compact
  end
  
  def _bodydata(event_data)

    event_name = event_data.dig("metadata", "event_name")
    body = event_data['body']
  
    case event_name
    
    when 'account_notification_created'

      bodydata = {
        account_notification_id:  body['account_notification_id']&.to_i,
        subject:                  body['subject']&.to_s,
        message:                  body['message']&.to_s,
        icon:                     body['icon']&.to_s,
        start_at:                 body['start_at'].nil? ? nil : default_timezone(body['start_at']),
        end_at:                   body['end_at'].nil? ? nil : default_timezone(body['end_at']),
      }

    when 'asset_accessed'

      bodydata = {
        asset_id:       body['asset_id']&.to_i,
        asset_name:     body['asset_name']&.to_s,
        asset_type:     body['asset_type']&.to_s,
        asset_subtype:  body['asset_subtype']&.to_s,
        category:       body['category']&.to_s,
        role:           body['role']&.to_s,
        level:          body['level']&.to_s,
        filename:       body['filename']&.to_s,
        display_name:   body['display_name']&.to_s,
        domain:         body['domain']&.to_s,
        url:            body['url']&.to_s,
        enrollment_id:  body['enrollment_id']&.to_i,
        section_id:     body['section_id']&.to_i,
      }

    when 'assignment_created'

      bodydata = {
        assignment_id:          body['assignment_id']&.to_i,
        context_id:             body['context_id']&.to_i,
        context_type:           body['context_type']&.to_s,
        context_uuid:           body['context_uuid']&.to_s,
        assignment_group_id:    body['assignment_group_id']&.to_i,
        workflow_state:         body['workflow_state']&.to_s,
        title:                  body['title']&.to_s,
        description:            body['description']&.to_s,
        due_at:                 body['due_at'].nil? ? nil : default_timezone(body['due_at']),
        unlock_at:              body['unlock_at'].nil? ? nil : default_timezone(body['unlock_at']),
        lock_at:                body['lock_at'].nil? ? nil : default_timezone(body['lock_at']),
        updated_at:             body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
        points_possible:        body['points_possible']&.to_f,
        lti_assignment_id:      body['lti_assignment_id']&.to_s,
        lti_resource_link_id:   body['lti_resource_link_id']&.to_s,
        lti_resource_link_id_duplicated_from: body['lti_resource_link_id_duplicated_from']&.to_s,
        submission_types:       body['submission_types']&.to_s,
      }

    when 'assignment_updated'

      bodydata = {
        assignment_id:          body['assignment_id']&.to_i,
        context_id:             body['context_id']&.to_i,
        context_type:           body['context_type']&.to_s,
        context_uuid:           body['context_uuid']&.to_s,
        assignment_group_id:    body['assignment_group_id']&.to_i,
        workflow_state:         body['workflow_state']&.to_s,
        title:                  body['title']&.to_s,
        description:            body['description']&.to_s,
        due_at:                 body['due_at'].nil? ? nil : default_timezone(body['due_at']),
        unlock_at:              body['unlock_at'].nil? ? nil : default_timezone(body['unlock_at']),
        lock_at:                body['lock_at'].nil? ? nil : default_timezone(body['lock_at']),
        updated_at:             body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
        points_possible:        body['points_possible']&.to_f,
        lti_assignment_id:      body['lti_assignment_id']&.to_s,
        lti_resource_link_id:   body['lti_resource_link_id']&.to_s,
        lti_resource_link_id_duplicated_from: body['lti_resource_link_id_duplicated_from']&.to_s,
        submission_types:       body['submission_types']&.to_s,
      }

    when 'assignment_group_created'

      bodydata = {
        assignment_group_id:   body['assignment_group_id']&.to_i,
        context_id:            body['context_id']&.to_i,
        context_type:          body['context_type']&.to_s,
        context_role:          body['context_role']&.to_s,
        name:                  body['name']&.to_s,
        position:              body['position']&.to_i,
        group_weight:          body['group_weight']&.to_f,
        sis_source_id:         body['sis_source_id']&.to_s,
        integration_data:      body['integration_data']&.to_s,
        rules:                 body['rules']&.to_s,
        workflow_state:         body['workflow_state']&.to_s,
      }

    when 'assignment_group_updated'

      bodydata = {
        assignment_group_id:   body['assignment_group_id']&.to_i,
        context_id:            body['context_id']&.to_i,
        context_type:          body['context_type']&.to_s,
        context_role:          body['context_role']&.to_s,
        name:                  body['name']&.to_s,
        position:              body['position']&.to_i,
        group_weight:          body['group_weight']&.to_f,
        sis_source_id:         body['sis_source_id']&.to_s,
        integration_data:      body['integration_data']&.to_s,
        rules:                 body['rules']&.to_s,
        workflow_state:         body['workflow_state']&.to_s,
      }
    
    when 'assignment_override_created'
      
      bodydata = {
        assignment_override_id: body['assignment_override_id']&.to_i,
        assignment_id:          body['assignment_id']&.to_i,
        due_at:                 body['due_at'].nil? ? nil : default_timezone(body['due_at']),
        all_day:                body['all_day']&.to_s,
        all_day_date:           body['all_day_date'].nil? ? nil : default_timezone(body['all_day_date']),
        unlock_at:              body['unlock_at'].nil? ? nil : default_timezone(body['unlock_at']),
        lock_at:                body['lock_at'].nil? ? nil : default_timezone(body['lock_at']),
        type:                   body['type']&.to_s,
        workflow_state:         body['workflow_state']&.to_s,
        course_section_id:      body['course_section_id']&.to_s,
      }

    when 'assignment_override_updated'

      bodydata = {
        assignment_override_id: body['assignment_override_id']&.to_i,
        assignment_id:          body['assignment_id']&.to_i,
        due_at:                 body['due_at'].nil? ? nil : default_timezone(body['due_at']),
        all_day:                body['all_day']&.to_s,
        all_day_date:           body['all_day_date'].nil? ? nil : default_timezone(body['all_day_date']),
        unlock_at:              body['unlock_at'].nil? ? nil : default_timezone(body['unlock_at']),
        lock_at:                body['lock_at'].nil? ? nil : default_timezone(body['lock_at']),
        type:                   body['type']&.to_s,
        workflow_state:         body['workflow_state']&.to_s,
        course_section_id:      body['course_section_id']&.to_s,
      }

    when 'attachment_created'

      bodydata = {
        attachment_id:  body['attachment_id']&.to_i,
        user_id:        body['user_id']&.to_i,
        display_name:   body['display_name']&.to_s,
        filename:       body['filename']&.to_s,
        folder_id:      body['folder_id']&.to_i,
        context_type:   body['context_type']&.to_s,
        context_id:     body['context_id']&.to_i,
        content_type:   body['content_type']&.to_s,
        unlock_at:      body['unlock_at'].nil? ? nil : default_timezone(body['unlock_at']),
        lock_at:        body['lock_at'].nil? ? nil : default_timezone(body['lock_at']),
        updated_at:     body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
      }

    when 'attachment_deleted'

      bodydata = {
        attachment_id:  body['attachment_id']&.to_i,
        user_id:        body['user_id']&.to_i,
        display_name:   body['display_name']&.to_s,
        filename:       body['filename']&.to_s,
        folder_id:      body['folder_id']&.to_i,
        context_type:   body['context_type']&.to_s,
        context_id:     body['context_id']&.to_i,
        content_type:   body['content_type']&.to_s,
        unlock_at:      body['unlock_at'].nil? ? nil : default_timezone(body['unlock_at']),
        lock_at:        body['lock_at'].nil? ? nil : default_timezone(body['lock_at']),
        updated_at:     body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
      }

    when 'attachment_updated'

      bodydata = {
        attachment_id:    body['attachment_id']&.to_i,
        user_id:          body['user_id']&.to_i,
        display_name:     body['display_name']&.to_s,
        old_display_name: body['old_display_name']&.to_s,
        folder_id:        body['folder_id']&.to_i,
        filename:         body['filename']&.to_s,
        context_type:     body['context_type']&.to_s,
        context_id:       body['context_id']&.to_i,
        content_type:     body['content_type']&.to_s,
        unlock_at:        body['unlock_at'].nil? ? nil : default_timezone(body['unlock_at']),
        lock_at:          body['lock_at'].nil? ? nil : default_timezone(body['lock_at']),
        updated_at:       body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
      }

    when 'content_migration_completed'

      bodydata = {
        content_migration_id: body['content_migration_id']&.to_i,
        context_id:           body['context_id']&.to_i,
        context_type:         body['context_type']&.to_s,
        lti_context_id:       body['lti_context_id']&.to_s,
        context_uuid:         body['context_uuid']&.to_s,
        import_quizzes_next:  body['import_quizzes_next']&.to_s,
      }

    when 'course_completed'

      bodydata = {
        # body progress
        requirement_count:            body['progress']['requirement_count'].nil? ? nil : body['progress']['requirement_count'].to_i,
        requirement_completed_count:  body['progress']['requirement_completed_count'].nil? ? nil : body['progress']['requirement_completed_count'].to_i,
        next_requirement_url:         body['progress']['next_requirement_url'].nil? ? nil : body['progress']['next_requirement_url'].to_s,
        completed_at:                 body['progress']['completed_at'].nil? ? nil : default_timezone(body['progress']['completed_at']),
        # body user
        user_id:                      body['user']['id'].nil? ? nil : body['user']['id'].to_i,
        user_name:                    body['user']['name'].nil? ? nil : body['user']['name'].to_s,
        user_email:                   body['user']['email'].nil? ? nil : body['user']['email'].to_s,
        # body course
        course_id:                    body['course']['id'].nil? ? nil : body['course']['id'].to_i,
        course_name:                  body['course']['name'].nil? ? nil : body['course']['name'].to_s,
      }

    when 'course_created'

      bodydata = {
        course_id:      body['course_id']&.to_i,
        uuid:           body['uuid']&.to_s,
        account_id:     body['account_id']&.to_i,
        name:           body['name']&.to_s,
        created_at:     body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        updated_at:     body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
        workflow_state: body['workflow_state']&.to_s,
      }

    when 'course_grade_change'

      bodydata = {
        user_id:                    body['user_id']&.to_i,  
        course_id:                  body['course_id']&.to_i,
        workflow_state:             body['workflow_state']&.to_s,
        created_at:                 body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        updated_at:                 body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
        current_score:              body['current_score']&.to_f,
        old_current_score:          body['old_current_score']&.to_f,
        final_score:                body['final_score']&.to_f,
        old_final_score:            body['old_final_score']&.to_f,
        unposted_current_score:     body['unposted_current_score']&.to_f,
        old_unposted_current_score: body['old_unposted_current_score']&.to_f,
        unposted_final_score:       body['unposted_final_score']&.to_f,
        old_unposted_final_score:   body['old_unposted_final_score']&.to_f,
      }

    when 'course_progress'

      bodydata = {
        # body progress
        error_message:                body['progress']['error'].nil? ? nil : body['progress']['error']['message'].to_s,
        requirement_count:            body['progress']['requirement_count'].nil? ? nil : body['progress']['requirement_count'].to_i,
        requirement_completed_count:  body['progress']['requirement_completed_count'].nil? ? nil : body['progress']['requirement_completed_count'].to_i,
        next_requirement_url:         body['progress']['next_requirement_url'].nil? ? nil : body['progress']['next_requirement_url'].to_s,
        completed_at:                 body['progress']['completed_at'].nil? ? nil : default_timezone(body['progress']['completed_at']),
        # body user
        user_id:                      body['user']['id'].nil? ? nil : body['user']['id'].to_i,
        user_name:                    body['user']['name'].nil? ? nil : body['user']['name'].to_s,
        user_email:                   body['user']['email'].nil? ? nil : body['user']['email'].to_s,
        # body course
        course_id:                    body['course']['id'].nil? ? nil : body['course']['id'].to_i,
        course_name:                  body['course']['name'].nil? ? nil : body['course']['name'].to_s,
      }      

    when 'course_section_created'

      bodydata = {
        course_section_id:                      body['course_section_id']&.to_i,
        sis_source_id:                          body['sis_source_id']&.to_s,
        sis_batch_id:                           body['sis_batch_id']&.to_s,
        course_id:                              body['course_id']&.to_i,
        root_account_id:                        body['root_account_id']&.to_i,
        enrollment_term_id:                     body['enrollment_term_id']&.to_s,
        name:                                   body['name']&.to_s,
        default_section:                        body['default_section']&.to_s,
        accepting_enrollments:                  body['accepting_enrollments']&.to_s,
        can_manually_enroll:                    body['can_manually_enroll']&.to_s,
        start_at:                               body['start_at'].nil? ? nil : default_timezone(body['start_at']),
        end_at:                                 body['end_at'].nil? ? nil : default_timezone(body['end_at']),
        workflow_state:                         body['workflow_state']&.to_s,
        restrict_enrollments_to_section_dates:  body['restrict_enrollments_to_section_dates']&.to_s,
        nonxlist_course_id:                     body['nonxlist_course_id']&.to_s,
        stuck_sis_fields:                       body['stuck_sis_fields'].length == 0 ? nil : body['stuck_sis_fields'].join(','),
        integration_id:                         body['integration_id']&.to_s,
      }

    when 'course_section_updated'

      bodydata = {
        course_section_id:                      body['course_section_id']&.to_i,
        sis_source_id:                          body['sis_source_id']&.to_s,
        sis_batch_id:                           body['sis_batch_id']&.to_s,
        course_id:                              body['course_id']&.to_i,
        root_account_id:                        body['root_account_id']&.to_i,
        enrollment_term_id:                     body['enrollment_term_id']&.to_s,
        name:                                   body['name']&.to_s,
        default_section:                        body['default_section']&.to_s,
        accepting_enrollments:                  body['accepting_enrollments']&.to_s,
        can_manually_enroll:                    body['can_manually_enroll']&.to_s,
        start_at:                               body['start_at'].nil? ? nil : default_timezone(body['start_at']),
        end_at:                                 body['end_at'].nil? ? nil : default_timezone(body['end_at']),
        workflow_state:                         body['workflow_state']&.to_s,
        restrict_enrollments_to_section_dates:  body['restrict_enrollments_to_section_dates']&.to_s,
        nonxlist_course_id:                     body['nonxlist_course_id']&.to_s,
        stuck_sis_fields:                       body['stuck_sis_fields'].length == 0 ? nil : body['stuck_sis_fields'].join(','),
        integration_id:                         body['integration_id']&.to_s,
      }

    when 'course_updated'

      bodydata = {
        course_id:      body['course_id']&.to_i,
        account_id:     body['account_id']&.to_i,
        uuid:           body['uuid']&.to_s,
        name:           body['name']&.to_s,
        created_at:     body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        updated_at:     body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
        workflow_state: body['workflow_state']&.to_s,
      }

    when 'discussion_entry_created'

      bodydata = {
        user_id:                            body['user_id']&.to_i,
        created_at:                         body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        discussion_entry_id:                body['discussion_entry_id']&.to_i,
        parent_discussion_entry_id:         body['parent_discussion_entry_id']&.to_i,
        parent_discussion_entry_author_id:  body['parent_discussion_entry_author_id']&.to_i,
        discussion_topic_id:                body['discussion_topic_id']&.to_i,
        text:                               body['text']&.to_s,
      }
    
    when 'discussion_entry_submitted'

      bodydata = {
        user_id:                      body['user_id']&.to_i,
        created_at:                   body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        discussion_entry_id:          body['discussion_entry_id']&.to_i,
        discussion_topic_id:          body['discussion_topic_id']&.to_i,
        text:                         body['text']&.to_s,
        parent_discussion_entry_id:   body['parent_discussion_entry_id']&.to_i,
        assignment_id:                body['assignment_id']&.to_i,
        submission_id:                body['submission_id']&.to_i,
      }

    when 'discussion_topic_created'

      bodydata = {
        discussion_topic_id:  body['discussion_topic_id']&.to_i,
        is_announcement:      body['is_announcement']&.to_s,
        title:                body['title']&.to_s,
        body:                 body['body']&.to_s,
        assignment_id:        body['assignment_id']&.to_i, 
        context_id:           body['context_id']&.to_i,
        context_type:         body['context_type']&.to_s,
        workflow_state:       body['workflow_state']&.to_s,
        lock_at:              body['lock_at']&.to_s,
        updated_at:           body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
      }
    
    when 'discussion_topic_updated'

      bodydata = {
        discussion_topic_id:  body['discussion_topic_id']&.to_i,
        is_announcement:      body['is_announcement']&.to_s,
        title:                body['title']&.to_s,
        body:                 body['body']&.to_s,
        assignment_id:        body['body']&.to_i, 
        context_id:           body['context_id']&.to_i,
        context_type:         body['context_type']&.to_s,
        workflow_state:       body['workflow_state']&.to_s,
        lock_at:              body['lock_at']&.to_s,
        updated_at:           body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
      }

    when 'enrollment_created'

      bodydata = {
        enrollment_id:                       body['enrollment_id']&.to_i,
        course_id:                           body['course_id']&.to_i,
        user_id:                             body['user_id']&.to_i,
        user_name:                           body['user_name']&.to_s,
        type:                                body['type']&.to_s,
        created_at:                          body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        updated_at:                          body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
        limit_privileges_to_course_section:  body['limit_privileges_to_course_section']&.to_s,
        course_section_id:                   body['course_section_id']&.to_i,
        associated_user_id:                  body['associated_user_id']&.to_i,
        workflow_state:                      body['workflow_state']&.to_s,
      }

    when 'enrollment_state_created'

      bodydata = {
        enrollment_id:          body['enrollment_id']&.to_i,
        state:                  body['state']&.to_s,
        state_started_at:       body['state_started_at'].nil? ? nil : default_timezone(body['state_started_at']),
        state_is_current:       body['state_is_current']&.to_s,
        state_valid_until:      body['state_valid_until'].nil? ? nil : default_timezone(body['state_valid_until']),
        restricted_access:      body['restricted_access']&.to_s,
        access_is_current:      body['access_is_current']&.to_s,
        state_invalidated_at:   body['state_invalidated_at'].nil? ? nil : default_timezone(body['state_invalidated_at']),
        state_recalculated_at:  body['state_recalculated_at'].nil? ? nil : default_timezone(body['state_recalculated_at']),
        access_invalidated_at:  body['access_invalidated_at'].nil? ? nil : default_timezone(body['access_invalidated_at']),
        access_recalculated_at: body['access_recalculated_at'].nil? ? nil : default_timezone(body['access_recalculated_at']),
      }

    when 'enrollment_state_updated'

      bodydata = {
        enrollment_id:          body['enrollment_id']&.to_i,
        state:                  body['state']&.to_s,
        state_started_at:       body['state_started_at'].nil? ? nil : default_timezone(body['state_started_at']),
        state_is_current:       body['state_is_current']&.to_s,
        state_valid_until:      body['state_valid_until'].nil? ? nil : default_timezone(body['state_valid_until']),
        restricted_access:      body['restricted_access']&.to_s,
        access_is_current:      body['access_is_current']&.to_s,
        state_invalidated_at:   body['state_invalidated_at'].nil? ? nil : default_timezone(body['state_invalidated_at']),
        state_recalculated_at:  body['state_recalculated_at'].nil? ? nil : default_timezone(body['state_recalculated_at']),
        access_invalidated_at:  body['access_invalidated_at'].nil? ? nil : default_timezone(body['access_invalidated_at']),
        access_recalculated_at: body['access_recalculated_at'].nil? ? nil : default_timezone(body['access_recalculated_at']),
      }

    when 'enrollment_updated'

      bodydata = {
        enrollment_id:                      body['enrollment_id']&.to_i,
        course_id:                          body['course_id']&.to_i,
        user_id:                            body['user_id']&.to_i,
        user_name:                          body['user_name']&.to_s,
        type:                               body['type']&.to_s,
        created_at:                         body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        updated_at:                         body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
        limit_privileges_to_course_section: body['limit_privileges_to_course_section']&.to_s,
        course_section_id:                  body['course_section_id']&.to_i,
        associated_user_id:                 body['associated_user_id']&.to_i,
        workflow_state:                     body['workflow_state']&.to_s,
      }

    when 'grade_change'

      bodydata = {
        submission_id:        body['submission_id']&.to_i,
        assignment_id:        body['assignment_id']&.to_i,
        assignment_name:      body['assignment_name']&.to_s,
        grade:                body['grade']&.to_s,
        old_grade:            body['old_grade']&.to_s,
        score:                body['score']&.to_f,
        old_score:            body['old_score']&.to_f,
        points_possible:      body['points_possible']&.to_f,
        old_points_possible:  body['old_points_possible']&.to_f,
        grader_id:            body['grader_id']&.to_i,
        student_id:           body['student_id']&.to_i,
        student_sis_id:       body['student_sis_id']&.to_s,
        user_id:              body['user_id']&.to_i,
        grading_complete:     body['grading_complete']&.to_s,
        muted:                body['muted']&.to_s
      }

    when 'group_category_created'

      bodydata = {
        group_category_id:    body['group_category_id']&.to_i,
        group_category_name:  body['group_category_name']&.to_s,
        context_id:           body['context_id']&.to_i,
        context_type:         body['context_type']&.to_s,
        group_limit:          body['group_limit']&.to_i,
      }

    when 'group_category_updated'

      bodydata = {
        group_category_id:    body['group_category_id']&.to_i,
        group_category_name:  body['group_category_name']&.to_s,
        context_id:           body['context_id']&.to_i,
        context_type:         body['context_type']&.to_s,
        group_limit:          body['group_limit']&.to_i,
      }

    when 'group_created'

      bodydata = {
        group_category_id:    body['group_category_id']&.to_i,
        group_category_name:  body['group_category_name']&.to_s,
        group_id:             body['group_id']&.to_i,
        group_name:           body['group_name']&.to_s,
        uuid:                 body['uuid']&.to_s,
        context_type:         body['context_type']&.to_s,
        context_id:           body['context_id']&.to_i,
        account_id:           body['account_id']&.to_i,
        workflow_state:       body['workflow_state']&.to_s,
        max_membership:       body['max_membership']&.to_i,
      }

    when 'group_membership_created'

      bodydata = {
        group_membership_id:  body['group_membership_id']&.to_i,
        user_id:              body['user_id']&.to_i,
        group_id:             body['group_id']&.to_i,
        group_name:           body['group_name']&.to_s,
        group_category_id:    body['group_category_id']&.to_i,
        group_category_name:  body['group_category_name']&.to_s,
        workflow_state:       body['workflow_state']&.to_s,
      }

    when 'group_membership_updated'

      bodydata = {
        group_membership_id:  body['group_membership_id']&.to_i,
        user_id:              body['user_id']&.to_i,
        group_id:             body['group_id']&.to_i,
        group_name:           body['group_name']&.to_s,
        group_category_id:    body['group_category_id']&.to_i,
        group_category_name:  body['group_category_name']&.to_s,
        workflow_state:       body['workflow_state']&.to_s,
      }

    when 'group_updated'

      bodydata = {
        group_category_id:    body['group_category_id']&.to_i,
        group_category_name:  body['group_category_name']&.to_s,
        group_id:             body['group_id']&.to_i,
        group_name:           body['group_name']&.to_s,
        uuid:                 body['uuid']&.to_s,
        context_type:         body['context_type']&.to_s,
        context_id:           body['context_id']&.to_i,
        account_id:           body['account_id']&.to_i,
        workflow_state:       body['workflow_state']&.to_s,
        max_membership:       body['max_membership']&.to_i,
      }
    
    when 'learning_outcome_created'

      bodydata = {
        learning_outcome_id:              body['learning_outcome_id']&.to_i,
        context_id:                       body['context_id']&.to_i,
        context_type:                     body['context_type']&.to_s,
        display_name:                     body['display_name']&.to_s,
        short_description:                body['short_description']&.to_s,
        description:                      body['description']&.to_s,
        vendor_guid:                      body['vendor_guid']&.to_s,
        calculation_method:               body['calculation_method']&.to_s,
        calculation_int:                  body['calculation_int']&.to_s,
        rubric_criterion_description:     body['rubric_criterion']['description']&.to_s,
        rubric_criterion_ratings:         body['rubric_criterion']['ratings']&.to_json.to_s,
        rubric_criterion_mastery_points:  body['rubric_criterion']['mastery_points']&.to_f,
        rubric_criterion_points_possible: body['rubric_criterion']['points_possible']&.to_f,
        title:                            body['title']&.to_s,
        workflow_state:                   body['workflow_state']&.to_s,
      }

    when 'learning_outcome_group_created'

      bodydata = {
        learning_outcome_group_id:  body['learning_outcome_group_id']&.to_i,
        context_id:                 body['context_id']&.to_i,
        context_type:               body['context_type']&.to_s,
        title:                      body['title']&.to_s,
        description:                body['description']&.to_s,
        vendor_guid:                body['vendor_guid']&.to_s,
        parent_outcome_group_id:    body['parent_outcome_group_id']&.to_i,
        workflow_state:             body['workflow_state']&.to_s,
      }

    when 'learning_outcome_group_updated'

      bodydata = {
        learning_outcome_group_id:  body['learning_outcome_group_id']&.to_i,
        context_id:                 body['context_id']&.to_i,
        context_type:               body['context_type']&.to_s,
        title:                      body['title']&.to_s,
        description:                body['description']&.to_s,
        vendor_guid:                body['vendor_guid']&.to_s,
        parent_outcome_group_id:    body['parent_outcome_group_id']&.to_i,
        workflow_state:             body['workflow_state']&.to_s,
        updated_at:                 body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
      }

    when 'learning_outcome_link_created'

      bodydata = {
        learning_outcome_link_id:   body['learning_outcome_link_id']&.to_i,
        learning_outcome_id:        body['learning_outcome_id']&.to_i,
        learning_outcome_group_id:  body['learning_outcome_group_id']&.to_i,
        context_id:                 body['context_id']&.to_i,
        context_type:               body['context_type']&.to_s,
        workflow_state:             body['workflow_state']&.to_s,
      }

    when 'learning_outcome_link_updated'

      bodydata = {
        learning_outcome_link_id:   body['learning_outcome_link_id']&.to_i,
        learning_outcome_id:        body['learning_outcome_id']&.to_i,
        learning_outcome_group_id:  body['learning_outcome_group_id']&.to_i,
        context_id:                 body['context_id']&.to_i,
        context_type:               body['context_type']&.to_s,
        workflow_state:             body['workflow_state']&.to_s,
        updated_at:                 body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
      }

    when 'learning_outcome_result_created'

      bodydata = {
        learning_outcome_id:  body['learning_outcome_id']&.to_i,
        mastery:              body['mastery']&.to_s,
        score:                body['score']&.to_f,
        created_at:           body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        attempt:              body['attempt']&.to_i,
        possible:             body['possible']&.to_f,
        original_score:       body['original_score']&.to_f,
        original_possible:    body['original_possible']&.to_f,
        original_mastery:     body['original_mastery']&.to_s,
        assessed_at:          body['assessed_at'].nil? ? nil : default_timezone(body['updated_at']),
        title:                body['title']&.to_s,
        percent:              body['percent']&.to_f,
      }

    when 'learning_outcome_result_updated'

      bodydata = {
        learning_outcome_id:  body['learning_outcome_id']&.to_i,
        mastery:              body['mastery']&.to_s,
        score:                body['score']&.to_f,
        created_at:           body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        attempt:              body['attempt']&.to_i,
        possible:             body['possible']&.to_f,
        original_score:       body['original_score']&.to_f,
        original_possible:    body['original_possible']&.to_f,
        original_mastery:     body['original_mastery']&.to_s,
        assessed_at:          body['assessed_at'].nil? ? nil : default_timezone(body['updated_at']),
        title:                body['title']&.to_s,
        percent:              body['percent']&.to_f,
        updated_at:           body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
      }

    when 'learning_outcome_updated'
    
      bodydata = {
        learning_outcome_id:              body['learning_outcome_id']&.to_i,
        context_id:                       body['context_id']&.to_i,
        context_type:                     body['context_type']&.to_s,
        display_name:                     body['display_name']&.to_s,
        short_description:                body['short_description']&.to_s,
        description:                      body['description']&.to_s,
        vendor_guid:                      body['vendor_guid']&.to_s,
        calculation_method:               body['calculation_method']&.to_s,
        calculation_int:                  body['calculation_int']&.to_i,
        rubric_criterion_description:     body['rubric_criterion']['description']&.to_s,
        rubric_criterion_ratings:         body['rubric_criterion']['ratings']&.to_json.to_s,
        rubric_criterion_mastery_points:  body['rubric_criterion']['mastery_points']&.to_f,
        rubric_criterion_points_possible: body['rubric_criterion']['points_possible']&.to_f,
        title:                            body['title']&.to_s,
        workflow_state:                   body['workflow_state']&.to_s,
        updated_at:                       body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
      }

    when 'logged_in'

      bodydata = {
        redirect_url: body['redirect_url']&.to_s,
      }
      
    when 'logged_out'

      bodydata = {}

    when 'module_created'

      bodydata = {
        module_id:      body['module_id']&.to_i,
        context_id:     body['context_id']&.to_i,
        context_type:   body['context_type']&.to_s,
        name:           body['name']&.to_s,
        position:       body['position']&.to_i,
        workflow_state: body['workflow_state']&.to_s,
      }

    when 'module_item_created'

      bodydata = {
        module_item_id: body['module_item_id']&.to_i,
        module_id:      body['module_id']&.to_i,
        context_id:     body['context_id']&.to_i,
        context_type:   body['context_type']&.to_s,
        position:       body['position']&.to_i,
        workflow_state: body['workflow_state']&.to_s,
      }

    when 'module_item_updated'

      bodydata = {
        module_item_id: body['module_item_id']&.to_i,
        module_id:      body['module_id']&.to_i,
        context_id:     body['context_id']&.to_i,
        context_type:   body['context_type']&.to_s,
        position:       body['position']&.to_i,
        workflow_state: body['workflow_state']&.to_s,
      }

    when 'module_updated'

      bodydata = {
        module_id:      body['module_id']&.to_i,
        context_id:     body['context_id']&.to_i,
        context_type:   body['context_type']&.to_s,
        name:           body['name']&.to_s,
        position:       body['position']&.to_i,
        workflow_state: body['workflow_state']&.to_s,
      }

    when 'plagiarism_resubmit'
      bodydata = {
        submission_id:      body['submission_id']&.to_i,
        assignment_id:      body['assignment_id']&.to_i,
        user_id:            body['user_id']&.to_i,
        submitted_at:       body['submitted_at'].nil? ? nil : default_timezone(body['submitted_at']),
        lti_user_id:        body['lti_user_id']&.to_s,
        graded_at:          body['graded_at'].nil? ? nil : default_timezone(body['graded_at']),
        updated_at:         body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
        score:              body['score']&.to_s,
        grade:              body['grade']&.to_s,
        submission_type:    body['submission_type']&.to_s,
        body:               body['body']&.to_s,
        url:                body['url']&.to_s,
        attempt:            body['attempt']&.to_i,
        lti_assignment_id:  body['lti_assignment_id']&.to_s,
        group_id:           body['group_id']&.to_i,
        late:               body['late']&.to_s,
        missing:            body['missing']&.to_s,
      }

    when 'quiz_export_complete'

      bodydata = {
        assignment_resource_link_id:  body['assignment']['resource_link_id'].nil? ? nil : body['assignment']['resource_link_id'].to_s,
        assignment_title:             body['assignment']['title'].nil? ? nil : body['assignment']['title'].to_s,
        assignment_context_title:     body['assignment']['context_title'].nil? ? nil : body['assignment']['context_title'].to_s,
        assignment_course_uuid:       body['assignment']['course_uuid'].nil? ? nil : body['assignment']['course_uuid'].to_s,
        qti_export_url:               body['qti_export']['url'].nil? ? nil : body['qti_export']['url'].to_s,
      }

    when 'quiz_submitted'

      bodydata = {
        submission_id:  body['submission_id']&.to_i,
        quiz_id:        body['quiz_id']&.to_i,
      }

    when 'quizzes.item_created'

      if body.key?('properties')
        properties = body['properties']
        properties_rich_content_editor = properties.key?('rich_content_editor') ? properties['rich_content_editor'].to_json.to_s : nil
        properties_show_word_count = properties.key?('show_word_count') ? properties['show_word_count'].to_json.to_s : nil
        properties_shuffle_rules = properties.key?('shuffle_rules') ? properties['shuffle_rules'].to_json.to_s : nil
        properties_spell_check = properties.key?('spell_check') ? properties['spell_check'].to_json.to_s : nil
        properties_word_limit_max = properties.key?('word_limit_max') ? properties['word_limit_max'].to_json.to_s : nil
        properties_word_limit_min = properties.key?('word_limit_min') ? properties['word_limit_min'].to_json.to_s : nil
        properties_word_limit = properties.key?('word_limit') ? properties['word_limit'].to_json.to_s : nil
        properties_display_answers_paragraph = properties.key?('display_answers_paragraph') ? properties['display_answers_paragraph'].to_json.to_s : nil
        properties_include_labels = properties.key?('include_labels') ? properties['include_labels'].to_json.to_s : nil
        properties_top_label = properties.key?('top_label') ? properties['top_label'].to_json.to_s : nil
        properties_bottom_label = properties.key?('bottom_label') ? properties['bottom_label'].to_json.to_s : nil
      end

      if body.key?('interaction_data')
        interaction_data = body['interaction_data']
        # "interaction_data": {
        #   "choices": [{
        #     "id": "9ee5b221-7a55-44fe-bda3-deb656463c61",
        #     "position": 1,
        #     "item_body": "<p>Color</p>"
        #   }, {
        #     "id": "9b87a79a-fc75-45bb-b578-09c120453d9a",
        #     "position": 3,
        #     "item_body": "<p>Value</p>"
        #   }, {
        #     "id": "dfdf22dc-d03c-457f-96ee-4c5a691373de",
        #     "position": 4,
        #     "item_body": "<p>Line</p>"
        #   }]
        # },
        interaction_data_choices = interaction_data.key?('choices') ? interaction_data['choices'].to_json.to_s : nil
        # "interaction_data": {
        #   "true_choice": "True",
        #   "false_choice": "False"
        # },
        interaction_data_true_choice = interaction_data.key?('true_choice') ? interaction_data['true_choice'].to_json.delete('"').to_s : nil
        interaction_data_false_choice = interaction_data.key?('false_choice') ? interaction_data['false_choice'].to_json.delete('"').to_s : nil
        # "interaction_data": {
        #   "essay": null,
        #   "rce": true,
        #   "spell_check": false,
        #   "word_count": false,
        #   "word_limit_enabled": false,
        #   "word_limit_max": null,
        #   "word_limit_min": null,
        #   "file_upload": false
        # },
        interaction_data_essay = interaction_data.key?('essay') ? interaction_data['essay'].to_json.to_s : nil
        interaction_data_rce = interaction_data.key?('rce') ? interaction_data['rce'].to_json.to_s : nil
        interaction_data_spell_check = interaction_data.key?('spell_check') ? interaction_data['spell_check'].to_json.to_s : nil
        interaction_data_word_count = interaction_data.key?('word_count') ? interaction_data['word_count'].to_json.to_s : nil
        interaction_data_word_limit_enabled = interaction_data.key?('word_limit_enabled') ? interaction_data['word_limit_enabled'].to_json.to_s : nil
        interaction_data_word_limit_max = interaction_data.key?('word_limit_max') ? interaction_data['word_limit_max'].to_json.to_s : nil
        interaction_data_word_limit_min = interaction_data.key?('word_limit_min') ? interaction_data['word_limit_min'].to_json.to_s : nil
        interaction_data_file_upload = interaction_data.key?('file_upload') ? interaction_data['file_upload'].to_json.to_s : nil
      end

      if body.key?('scoring_data')
        scoring_data = body['scoring_data'].nil? ? nil : body['scoring_data'].to_json.to_s
      end

      bodydata = {
        # body
        id:                                   body['id']&.to_i,
        title:                                body['title']&.to_s,
        label:                                body['label']&.to_s,
        item_body:                            body['item_body']&.to_s,
        properties_rich_content_editor:       properties_rich_content_editor,
        properties_show_word_count:           properties_show_word_count,
        properties_shuffle_rules:             properties_shuffle_rules,
        properties_spell_check:               properties_spell_check,
        properties_word_limit_max:            properties_word_limit_max,
        properties_word_limit_min:            properties_word_limit_min,
        properties_word_limit:                properties_word_limit,
        properties_display_answers_paragraph: properties_display_answers_paragraph,
        properties_include_labels:            properties_include_labels, 
        properties_top_label:                 properties_top_label,
        properties_bottom_label:              properties_bottom_label,
        interaction_data_choices:             interaction_data_choices,
        interaction_data_true_choice:         interaction_data_true_choice,
        interaction_data_false_choice:        interaction_data_false_choice,
        interaction_data_essay:               interaction_data_essay,
        interaction_data_rce:                 interaction_data_rce,
        interaction_data_spell_check:         interaction_data_spell_check,
        interaction_data_word_count:          interaction_data_word_count,
        interaction_data_word_limit_enabled:  interaction_data_word_limit_enabled,
        interaction_data_word_limit_max:      interaction_data_word_limit_max,
        interaction_data_word_limit_min:      interaction_data_word_limit_min,
        interaction_data_file_upload:         interaction_data_file_upload,
        user_response_type:                   body['user_response_type']&.to_s,
        outcome_alignment_set_guid:           body['outcome_alignment_set_guid']&.to_s,
        scoring_data:                         scoring_data,
        scoring_algorithm:                    body['scoring_algorithm']&.to_s,
      }

    when 'quizzes.item_updated'

      if body.key?('properties')
        properties = body['properties']
        properties_rich_content_editor = properties.key?('rich_content_editor') ? properties['rich_content_editor'].to_json.to_s : nil
        properties_show_word_count = properties.key?('show_word_count') ? properties['show_word_count'].to_json.to_s : nil
        properties_shuffle_rules = properties.key?('shuffle_rules') ? properties['shuffle_rules'].to_json.to_s : nil
        properties_spell_check = properties.key?('spell_check') ? properties['spell_check'].to_json.to_s : nil
        properties_word_limit_max = properties.key?('word_limit_max') ? properties['word_limit_max'].to_json.to_s : nil
        properties_word_limit_min = properties.key?('word_limit_min') ? properties['word_limit_min'].to_json.to_s : nil
        properties_word_limit = properties.key?('word_limit') ? properties['word_limit'].to_json.to_s : nil
        properties_display_answers_paragraph = properties.key?('display_answers_paragraph') ? properties['display_answers_paragraph'].to_json.to_s : nil
        properties_include_labels = properties.key?('include_labels') ? properties['include_labels'].to_json.to_s : nil
        properties_top_label = properties.key?('top_label') ? properties['top_label'].to_json.to_s : nil
        properties_bottom_label = properties.key?('bottom_label') ? properties['bottom_label'].to_json.to_s : nil
      end

      if body.key?('interaction_data')
        interaction_data = body['interaction_data']
        interaction_data_choices = interaction_data.key?('choices') ? interaction_data['choices'].to_json.to_s : nil
        interaction_data_true_choice = interaction_data.key?('true_choice') ? interaction_data['true_choice'].to_json.delete('"').to_s : nil
        interaction_data_false_choice = interaction_data.key?('false_choice') ? interaction_data['false_choice'].to_json.delete('"').to_s : nil
        interaction_data_essay = interaction_data.key?('essay') ? interaction_data['essay'].to_json.to_s : nil
        interaction_data_rce = interaction_data.key?('rce') ? interaction_data['rce'].to_json.to_s : nil
        interaction_data_spell_check = interaction_data.key?('spell_check') ? interaction_data['spell_check'].to_json.to_s : nil
        interaction_data_word_count = interaction_data.key?('word_count') ? interaction_data['word_count'].to_json.to_s : nil
        interaction_data_word_limit_enabled = interaction_data.key?('word_limit_enabled') ? interaction_data['word_limit_enabled'].to_json.to_s : nil
        interaction_data_word_limit_max = interaction_data.key?('word_limit_max') ? interaction_data['word_limit_max'].to_json.to_s : nil
        interaction_data_word_limit_min = interaction_data.key?('word_limit_min') ? interaction_data['word_limit_min'].to_json.to_s : nil
        interaction_data_file_upload = interaction_data.key?('file_upload') ? interaction_data['file_upload'].to_json.to_s : nil
      end

      if body.key?('scoring_data')
        scoring_data = body['scoring_data'].nil? ? nil : body['scoring_data'].to_json.to_s
      end

      bodydata = {
        # body
        id:                                   body['id']&.to_i,
        title:                                body['title']&.to_s,
        label:                                body['label']&.to_s,
        item_body:                            body['item_body']&.to_s,
        properties_rich_content_editor:       properties_rich_content_editor,
        properties_show_word_count:           properties_show_word_count,
        properties_shuffle_rules:             properties_shuffle_rules,
        properties_spell_check:               properties_spell_check,
        properties_word_limit_max:            properties_word_limit_max,
        properties_word_limit_min:            properties_word_limit_min,
        properties_word_limit:                properties_word_limit,
        properties_display_answers_paragraph: properties_display_answers_paragraph,
        properties_include_labels:            properties_include_labels, 
        properties_top_label:                 properties_top_label,
        properties_bottom_label:              properties_bottom_label,
        interaction_data_choices:             interaction_data_choices,
        interaction_data_true_choice:         interaction_data_true_choice,
        interaction_data_false_choice:        interaction_data_false_choice,
        interaction_data_essay:               interaction_data_essay,
        interaction_data_rce:                 interaction_data_rce,
        interaction_data_spell_check:         interaction_data_spell_check,
        interaction_data_word_count:          interaction_data_word_count,
        interaction_data_word_limit_enabled:  interaction_data_word_limit_enabled,
        interaction_data_word_limit_max:      interaction_data_word_limit_max,
        interaction_data_word_limit_min:      interaction_data_word_limit_min,
        interaction_data_file_upload:         interaction_data_file_upload,
        user_response_type:                   body['user_response_type']&.to_s,
        outcome_alignment_set_guid:           body['outcome_alignment_set_guid']&.to_s,
        scoring_data:                         scoring_data,
        scoring_algorithm:                    body['scoring_algorithm']&.to_s,
      }

    when 'quizzes-lti.grade_changed'

      bodydata = {
        user_uuid:      body['user_uuid']&.to_s,
        quiz_id:        body['quiz_id']&.to_i,
        score_to_keep:  body['score_to_keep']&.to_s,
      }

    when 'quizzes_next_quiz_duplicated'

      bodydata = {
        new_assignment_id:            body['new_assignment_id']&.to_i,
        original_course_uuid:         body['original_course_uuid']&.to_s,
        original_resource_link_id:    body['original_resource_link_id']&.to_s,
        new_course_uuid:              body['new_course_uuid']&.to_s,
        new_course_resource_link_id:  body['new_course_resource_link_id']&.to_s,
        new_course_id:                body['new_course_id']&.to_s,
        new_resource_link_id:         body['new_resource_link_id']&.to_s,
      }

    when 'quizzes.qti_import_completed'

      bodydata = {
        qti_type: body['qti_type']&.to_s,
        quiz_id:  body['quiz_id']&.to_i,
        success:  body['success']&.to_s,
      }

    when 'quizzes.quiz_clone_job_created'

      bodydata = {
        id:               body['id']&.to_s,
        original_quiz_id: body['original_quiz_id']&.to_s,
        status:           body['status']&.to_s,
      }

    when 'quizzes.quiz_clone_job_updated'

      bodydata = {
        id:               body['id']&.to_s,
        original_quiz_id: body['original_quiz_id']&.to_s,
        status:           body['status']&.to_s,
        cloned_quiz_id:   body['cloned_quiz_id']&.to_s,
      }

    when 'quizzes.quiz_created'

      bodydata = {
        id:                             body['id']&.to_i,
        title:                          body['title']&.to_s,
        instructions:                   body['instructions']&.to_s,
        context_id:                     body['context_id']&.to_i,
        owner:                          body['owner']&.to_s,
        has_time_limit:                 body['has_time_limit']&.to_s,
        due_at:                         body['due_at'].nil? ? nil : default_timezone(body['due_at']),
        lock_at:                        body['lock_at'].nil? ? nil : default_timezone(body['lock_at']),
        session_time_limit_in_seconds:  body['session_time_limit_in_seconds']&.to_i,
        shuffle_questions:              body['shuffle_questions']&.to_s,
        shuffle_answers:                body['shuffle_answers']&.to_s,
        status:                         body['status']&.to_s,
        outcome_alignment_set_guid:     body['outcome_alignment_set_guid']&.to_s,       
      }

    when 'quizzes.quiz_graded'

      bodydata = {
        quiz_session_id:        body['quiz_session_id']&.to_i,
        quiz_session_result_id: body['quiz_session_result_id']&.to_i,
        grader_id:              body['grader_id']&.to_i,
        grading_method:         body['grading_method']&.to_s,
        status:                 body['status']&.to_s,
        score:                  body['score']&.to_f,
        fudge_points:           body['fudge_points']&.to_s,
        points_possible:        body['points_possible']&.to_f,
        percentage:             body['percentage']&.to_f,
        created_at:             body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        updated_at:             body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
      }

    # when 'quizzes.quiz_session_graded'
    
    when 'quizzes.quiz_session_submitted'

      bodydata = {
        accepted_student_access_code_at:    body['accepted_student_access_code_at'].nil? ? nil : default_timezone(body['accepted_student_access_code_at']),
        allow_backtracking:                 body['allow_backtracking']&.to_s,
        attempt:                            body['attempt']&.to_i,
        authoritative_result_id:            body['authoritative_result_id']&.to_i,
        created_at:                         body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        end_at:                             body['end_at'].nil? ? nil : default_timezone(body['end_at']),
        grade_passback_guid:                body['grade_passback_guid']&.to_s,
        graded_url:                         body['graded_url']&.to_s,
        id:                                 body['id']&.to_i,
        invalidated_student_access_code_at: body['invalidated_student_access_code_at'].nil? ? nil : default_timezone(body['invalidated_student_access_code_at']),
        one_at_a_time_type:                 body['one_at_a_time_type']&.to_s,
        passback_url:                       body['passback_url']&.to_s,
        points_possible:                    body['points_possible']&.to_f,
        quiz_id:                            body['quiz_id']&.to_i,
        session_items_count:                body['session_items_count']&.to_i,
        start_at:                           body['start_at'].nil? ? nil : default_timezone(body['start_at']),
        status:                             body['status']&.to_s,
        submitted_at:                       body['submitted_at'].nil? ? nil : default_timezone(body['submitted_at']),
        updated_at:                         body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
        exclude_from_stats:                 body['exclude_from_stats']&.to_s,
      }

    when 'quizzes.quiz_session_ungraded'

      bodydata = {
        accepted_student_access_code_at:    body['accepted_student_access_code_at'].nil? ? nil : default_timezone(body['accepted_student_access_code_at']),
        allow_backtracking:                 body['allow_backtracking']&.to_s,
        attempt:                            body['attempt']&.to_i,
        authoritative_result_id:            body['authoritative_result_id']&.to_i,
        created_at:                         body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        end_at:                             body['end_at'].nil? ? nil : default_timezone(body['end_at']),
        grade_passback_guid:                body['grade_passback_guid']&.to_s,
        graded_url:                         body['graded_url']&.to_s,
        id:                                 body['id']&.to_i,
        invalidated_student_access_code_at: body['invalidated_student_access_code_at'].nil? ? nil : default_timezone(body['invalidated_student_access_code_at']),
        one_at_a_time_type:                 body['one_at_a_time_type']&.to_s,
        passback_url:                       body['passback_url']&.to_s,
        points_possible:                    body['points_possible']&.to_f,
        quiz_id:                            body['quiz_id']&.to_i,
        session_items_count:                body['session_items_count']&.to_i,
        start_at:                           body['start_at'].nil? ? nil : default_timezone(body['start_at']),
        status:                             body['status']&.to_s,
        submitted_at:                       body['submitted_at'].nil? ? nil : default_timezone(body['submitted_at']),
        updated_at:                         body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
        exclude_from_stats:                 body['exclude_from_stats']&.to_s,
      }

    when 'quizzes.quiz_updated'

      bodydata = {
        id:                             body['id']&.to_i,
        title:                          body['title']&.to_s,
        instructions:                   body['instructions']&.to_s,
        context_id:                     body['context_id']&.to_i,
        owner:                          body['owner']&.to_s,
        has_time_limit:                 body['has_time_limit']&.to_s,
        due_at:                         body['due_at'].nil? ? nil : default_timezone(body['due_at']),
        lock_at:                        body['lock_at'].nil? ? nil : default_timezone(body['lock_at']),
        session_time_limit_in_seconds:  body['session_time_limit_in_seconds']&.to_i,
        shuffle_answers:                body['shuffle_answers']&.to_s,
        shuffle_questions:              body['shuffle_questions']&.to_s,
        status:                         body['status']&.to_s,
        outcome_alignment_set_guid:     body['outcome_alignment_set_guid']&.to_s,
      }
    
    when 'quiz_caliper.quiz_session_created'
      
      bodydata = {
        created_at:     body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        end_at:         body['end_at'].nil? ? nil : default_timezone(body['end_at']),
        id:             body['id']&.to_i,
        start_at:       body['start_at'].nil? ? nil : default_timezone(body['start_at']),
        status:         body['status']&.to_s,
        submitted_at:   body['submitted_at'].nil? ? nil : default_timezone(body['submitted_at']),
      }
    
    when 'quiz_caliper.quiz_session_updated'
      
      bodydata = {
        created_at:     body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        end_at:         body['end_at'].nil? ? nil : default_timezone(body['end_at']),
        id:             body['id']&.to_i,
        start_at:       body['start_at'].nil? ? nil : default_timezone(body['start_at']),
        status:         body['status']&.to_s,
        submitted_at:   body['submitted_at'].nil? ? nil : default_timezone(body['submitted_at']),
      }
    
    when 'quiz_caliper.asset_accessed'
      
      bodydata = {
        id:      body['id']&.to_i,
        title:   body['title']&.to_s,
      }
    
    when 'quiz_caliper.quiz_created'
      
      bodydata = {
        id:      body['id']&.to_i,
        title:   body['title']&.to_s,
      }

    when 'quiz_caliper.quiz_started'
      
      bodydata = {
        id:      body['id']&.to_i,
        title:   body['title']&.to_s,
      }
    
    when 'quiz_caliper.quiz_submitted'
      
      bodydata = {
        id:      body['id']&.to_i,
        title:   body['title']&.to_s,
      }
    
    when 'quiz_caliper.quiz_updated'
      
      bodydata = {
        id:      body['id']&.to_i,
        title:   body['title']&.to_s,
      }
    
    when 'quiz_caliper.item_created'
      
      bodydata = {
        id:      body['id']&.to_i,
        title:   body['title']&.to_s,
        label:   body['label']&.to_s,
      }
    
    when 'quiz_caliper.item_updated'
      bodydata = {
        id:      body['id']&.to_i,
        title:   body['title']&.to_s,
        label:   body['label']&.to_s,
      }

    when 'sis_batch_created'

      bodydata = {
        sis_batch_id:     body['sis_batch_id']&.to_s,
        account_id:       body['account_id']&.to_i,
        workflow_state:   body['workflow_state']&.to_s,
      }

    when 'sis_batch_updated'

      bodydata = {
        sis_batch_id:     body['sis_batch_id']&.to_s,
        account_id:       body['account_id']&.to_i,
        workflow_state:   body['workflow_state']&.to_s,
      }

    when 'submission_comment_created'

      bodydata = {
        submission_comment_id:    body['submission_comment_id']&.to_i,
        submission_id:            body['submission_id']&.to_i,
        user_id:                  body['user_id']&.to_i,
        created_at:               body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        attachment_ids:           body['attachment_ids'].length == 0 ? nil : body['attachment_ids'].join(','),
        body:                     body['body']&.to_i,
      }

    when 'submission_created'

      bodydata = {
        submission_id:      body['submission_id']&.to_i,
        assignment_id:      body['assignment_id']&.to_i,
        user_id:            body['user_id']&.to_i,
        submitted_at:       body['submitted_at'].nil? ? nil : default_timezone(body['submitted_at']),
        lti_user_id:        body['lti_user_id']&.to_s,
        graded_at:          body['graded_at'].nil? ? nil : default_timezone(body['graded_at']),
        updated_at:         body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
        score:              body['score']&.to_f,
        grade:              body['grade']&.to_s,
        submission_type:    body['submission_type']&.to_s,
        body:               body['body']&.to_s,
        url:                body['url']&.to_s,
        attempt:            body['attempt']&.to_i,
        lti_assignment_id:  body['lti_assignment_id']&.to_s,
        group_id:           body['group_id']&.to_i,
        late:               body['late']&.to_s,
        missing:            body['missing']&.to_s,
      }

    when 'submission_updated'

      bodydata = {
        submission_id:      body['submission_id']&.to_i,
        assignment_id:      body['assignment_id']&.to_i,
        user_id:            body['user_id']&.to_i,
        submitted_at:       body['submitted_at'].nil? ? nil : default_timezone(body['submitted_at']),
        lti_user_id:        body['lti_user_id']&.to_s,
        lti_assignment_id:  body['lti_assignment_id']&.to_s,
        graded_at:          body['graded_at'].nil? ? nil : default_timezone(body['graded_at']),
        updated_at:         body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
        score:              body['score']&.to_f,
        grade:              body['grade']&.to_s,
        submission_type:    body['submission_type']&.to_s,
        body:               body['body']&.to_s,
        url:                body['url']&.to_s,
        attempt:            body['attempt']&.to_i,
        group_id:           body['group_id']&.to_i,
        late:               body['late']&.to_s,
        missing:            body['missing']&.to_s,
      }

    when 'syllabus_updated'

      bodydata = {
        course_id:          body['course_id']&.to_i,
        syllabus_body:      body['syllabus_body']&.to_s,
        old_syllabus_body:  body['old_syllabus_body']&.to_s,
      }

    when 'user_account_association_created'

      bodydata = {
        user_id:      body['user_id']&.to_i,
        account_id:   body['account_id']&.to_i,
        account_uuid: body['account_uuid']&.to_s,
        created_at:   body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        updated_at:   body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
        is_admin:     body['is_admin']&.to_s,
      }

    when 'user_created'

      bodydata = {
        user_id:        body['user_id']&.to_i,
        uuid:           body['uuid']&.to_s,
        name:           body['name']&.to_s,
        short_name:     body['short_name']&.to_s,
        workflow_state: body['workflow_state']&.to_s,
        created_at:     body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        updated_at:     body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
        user_login:     body['user_login']&.to_s,
        user_sis_id:    body['user_sis_id']&.to_s,
      }

    when 'user_updated'

      bodydata = {
        user_id:        body['user_id']&.to_i,
        uuid:           body['uuid']&.to_s,
        name:           body['name']&.to_s,
        short_name:     body['short_name']&.to_s,
        workflow_state: body['workflow_state']&.to_s,
        created_at:     body['created_at'].nil? ? nil : default_timezone(body['created_at']),
        updated_at:     body['updated_at'].nil? ? nil : default_timezone(body['updated_at']),
        user_login:     body['user_login']&.to_s,
        user_sis_id:    body['user_sis_id']&.to_s,
      }

    when 'wiki_page_created'

      bodydata = {
        wiki_page_id: body['wiki_page_id']&.to_i,
        title:        body['title']&.to_s,
        body:         body['body']&.to_s,
      }

    when 'wiki_page_deleted'

      bodydata = {
        wiki_page_id: body['wiki_page_id']&.to_i,
        title:        body['title']&.to_s,
      }

    when 'wiki_page_updated'

      bodydata = {
        wiki_page_id: body['wiki_page_id']&.to_i,
        title:        body['title']&.to_s,
        body:         body['body']&.to_s,
        old_title:    body['old_title']&.to_s,
        old_body:     body['old_body']&.to_s,
      }

    # catch and save events, we don't have configured or we aren't expecting
    else
      collect_unknown(event_name, event_data)
      # return if the message cannot be prepped for import
      return
    end

    # return parsed event bodydata
    bodydata
  end

  def _canvas(event_data)
    meta = _metadata(event_data)
    body = _bodydata(event_data)

    # check if we missed any new data
    missing_meta(event_data, meta)
    missing_body(event_data, body)

    # merge metadata and bodydata, prevent duplicate fields
    # return event data - parsed, flattened, ready for sql
    meta.merge!(body)
  end
end