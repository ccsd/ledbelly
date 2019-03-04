module CanvasRawEvents

  # collect and return all possible message data fields
  def metadata(meta)
    {
      client_ip:              meta['client_ip'].nil? ? nil : meta['client_ip'].to_s,
      context_id_meta:        meta['context_id'].nil? ? nil : meta['context_id'].to_i,
      context_role_meta:      meta['context_role'].nil? ? nil : meta['context_role'].to_s,
      context_type_meta:      meta['context_type'].nil? ? nil : meta['context_type'].to_s,
      event_name:             meta['event_name'].nil? ? nil : meta['event_name'].to_s,
      event_time:             meta['event_time'].nil? ? nil : Time.parse(meta['event_time']).utc.strftime(TIME_FORMAT).to_s,
      hostname:               meta['hostname'].nil? ? nil : meta['hostname'].to_s,
      job_id:                 meta['job_id'].nil? ? nil : meta['job_id'].to_i,
      job_tag:                meta['job_tag'].nil? ? nil : meta['job_tag'].to_s,
      producer:               meta['producer'].nil? ? nil : meta['producer'].to_s,
      real_user_id:           meta['real_user_id'].nil? ? nil : meta['real_user_id'].to_i,
      request_id:             meta['request_id'].nil? ? nil : meta['request_id'].to_s,
      root_account_id:        meta['root_account_id'].nil? ? nil : meta['root_account_id'].to_i,
      root_account_lti_guid:  meta['root_account_lti_guid'].nil? ? nil : meta['root_account_lti_guid'].to_s,
      root_account_uuid:      meta['root_account_uuid'].nil? ? nil : meta['root_account_uuid'].to_s,
      session_id:             meta['session_id'].nil? ? nil : meta['session_id'].to_s,
      user_account_id:        meta['user_account_id'].nil? ? nil : meta['user_account_id'].to_i,
      user_agent:             meta['user_agent'].nil? ? nil : meta['user_agent'].to_s,
      user_id_meta:           meta['user_id'].nil? ? nil : meta['user_id'].to_i,
      user_login:             meta['user_login'].nil? ? nil : meta['user_login'].to_s,
      user_sis_id:            meta['user_sis_id'].nil? ? nil : meta['user_sis_id'].to_s,
    }.compact
  end

  def bodycount(event_data, bodydata)
    if event_data['body'].keys.count > bodydata.keys.count

      ed = event_data.clone
      md = ed['metadata']
      eb = ed['body']
      bd = bodydata.clone.stringify_keys

      # compare the original message body with the fields for import, what keys are in one hash only?
      test = (eb.keys - bd.keys) | (bd.keys - eb.keys)
      flagged = false
      # check the missing keys
      test.each do |k|
        # if the missing key is in the metadata
        if md.key?(k)
          # compare the values, flag if they aren't the same
          if eb[k] == md[k]
            flagged = true
          end
        # the missing key is not in the metadata, flag, because we found new data
        else
          flagged = true
        end
      end
      if flagged == true
        err = %W[
            \n#{event_data['metadata']['event_name']} data was missed, because it's not explicitly defined for importing\n
            msg-body: #{event_data['body'].keys.count} vs body-set #{bodydata.keys.count}\n
            #{event_data.to_json}\n
            ----\n
            fields missing: #{test}
        ]
        # store in log file
        open('log/event-missing-data.log', 'a') do |f|
          f << err
          f << "\n\n"
        end
        puts err
      end
    end
  end

  def canvas_raw(event_name, event_time, event_data)

    case event_name

    when 'account_notification_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        account_notification_id:  body['account_notification_id'].nil? ? nil : body['account_notification_id'].to_i,
        subject:                  body['subject'].nil? ? nil : body['subject'].to_s,
        message:                  body['message'].nil? ? nil : body['message'].to_s,
        icon:                     body['icon'].nil? ? nil : body['icon'].to_s,
        start_at:                 body['start_at'].nil? ? nil : Time.parse(body['start_at']).utc.strftime(TIME_FORMAT).to_s,
        end_at:                   body['end_at'].nil? ? nil : Time.parse(body['end_at']).utc.strftime(TIME_FORMAT).to_s,
      }

    when 'asset_accessed'

      # metadata fields
      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      # body fields
      bodydata = {
        asset_id:       body['asset_id'].nil? ? nil : body['asset_id'].to_i,
        asset_type:     body['asset_type'].nil? ? nil : body['asset_type'].to_s,
        asset_subtype:  body['asset_subtype'].nil? ? nil : body['asset_subtype'].to_s,
        category:       body['category'].nil? ? nil : body['category'].to_s,
        role:           body['role'].nil? ? nil : body['role'].to_s,
        level:          body['level'].nil? ? nil : body['level'].to_s
      }

    when 'assignment_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        assignment_id:          body['assignment_id'].nil? ? nil : body['assignment_id'].to_i,
        context_id:             body['context_id'].nil? ? nil : body['context_id'].to_i,
        context_type:           body['context_type'].nil? ? nil : body['context_type'].to_s,
        context_uuid:           body['context_uuid'].nil? ? nil : body['context_uuid'].to_s,
        assignment_group_id:    body['assignment_group_id'].nil? ? nil : body['assignment_group_id'].to_i,
        workflow_state:         body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
        title:                  body['title'].nil? ? nil : body['title'].to_s,
        description:            body['description'].nil? ? nil : body['description'].to_s,
        due_at:                 body['due_at'].nil? ? nil : Time.parse(body['due_at']).utc.strftime(TIME_FORMAT).to_s,
        unlock_at:              body['unlock_at'].nil? ? nil : Time.parse(body['unlock_at']).utc.strftime(TIME_FORMAT).to_s,
        lock_at:                body['lock_at'].nil? ? nil : Time.parse(body['lock_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:             body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
        points_possible:        body['points_possible'].nil? ? nil : body['points_possible'].to_f,
        lti_assignment_id:      body['lti_assignment_id'].nil? ? nil : body['lti_assignment_id'].to_s,
        lti_resource_link_id:   body['lti_resource_link_id'].nil? ? nil : body['lti_resource_link_id'].to_s,
        lti_resource_link_id_duplicated_from:  body['lti_resource_link_id_duplicated_from'].nil? ? nil : body['lti_resource_link_id_duplicated_from'].to_s,
      }

    when 'assignment_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        assignment_id:          body['assignment_id'].nil? ? nil : body['assignment_id'].to_i,
        context_id:             body['context_id'].nil? ? nil : body['context_id'].to_i,
        context_type:           body['context_type'].nil? ? nil : body['context_type'].to_s,
        context_uuid:           body['context_uuid'].nil? ? nil : body['context_uuid'].to_s,
        assignment_group_id:    body['assignment_group_id'].nil? ? nil : body['assignment_group_id'].to_i,
        workflow_state:         body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
        title:                  body['title'].nil? ? nil : body['title'].to_s,
        description:            body['description'].nil? ? nil : body['description'].to_s,
        due_at:                 body['due_at'].nil? ? nil : Time.parse(body['due_at']).utc.strftime(TIME_FORMAT).to_s,
        unlock_at:              body['unlock_at'].nil? ? nil : Time.parse(body['unlock_at']).utc.strftime(TIME_FORMAT).to_s,
        lock_at:                body['lock_at'].nil? ? nil : Time.parse(body['lock_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:             body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
        points_possible:        body['points_possible'].nil? ? nil : body['points_possible'].to_f,
        lti_assignment_id:      body['lti_assignment_id'].nil? ? nil : body['lti_assignment_id'].to_s,
        lti_resource_link_id:   body['lti_resource_link_id'].nil? ? nil : body['lti_resource_link_id'].to_s,
        lti_resource_link_id_duplicated_from:  body['lti_resource_link_id_duplicated_from'].nil? ? nil : body['lti_resource_link_id_duplicated_from'].to_s,
      }

    when 'assignment_group_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        assignment_group_id:   body['assignment_group_id'].nil? ? nil : body['assignment_group_id'].to_i,
        context_id:            body['context_id'].nil? ? nil : body['context_id'].to_i,
        context_type:          body['context_type'].nil? ? nil : body['context_type'].to_s,
        context_role:          body['context_role'].nil? ? nil : body['context_role'].to_s,
        name:                  body['name'].nil? ? nil : body['name'].to_s,
        position:              body['position'].nil? ? nil : body['position'].to_i,
        group_weight:          body['group_weight'].nil? ? nil : body['group_weight'].to_f,
        sis_source_id:         body['sis_source_id'].nil? ? nil : body['sis_source_id'].to_s,
        integration_data:      body['integration_data'].nil? ? nil : body['integration_data'].to_s,
        rules:                 body['rules'].nil? ? nil : body['rules'].to_s,
      }

    when 'assignment_group_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        assignment_group_id:   body['assignment_group_id'].nil? ? nil : body['assignment_group_id'].to_i,
        context_id:            body['context_id'].nil? ? nil : body['context_id'].to_i,
        context_type:          body['context_type'].nil? ? nil : body['context_type'].to_s,
        context_role:          body['context_role'].nil? ? nil : body['context_role'].to_s,
        name:                  body['name'].nil? ? nil : body['name'].to_s,
        position:              body['position'].nil? ? nil : body['position'].to_i,
        group_weight:          body['group_weight'].nil? ? nil : body['group_weight'].to_f,
        sis_source_id:         body['sis_source_id'].nil? ? nil : body['sis_source_id'].to_s,
        integration_data:      body['integration_data'].nil? ? nil : body['integration_data'].to_s,
        rules:                 body['rules'].nil? ? nil : body['rules'].to_s,
      }

    when 'attachment_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        attachment_id:  body['attachment_id'].nil? ? nil : body['attachment_id'].to_i,
        user_id:        body['user_id'].nil? ? nil : body['user_id'].to_i,
        display_name:   body['display_name'].nil? ? nil : body['display_name'].to_s,
        filename:       body['filename'].nil? ? nil : body['filename'].to_s,
        folder_id:      body['folder_id'].nil? ? nil : body['folder_id'].to_i,
        context_type:   body['context_type'].nil? ? nil : body['context_type'].to_s,
        context_id:     body['context_id'].nil? ? nil : body['context_id'].to_i,
        content_type:   body['content_type'].nil? ? nil : body['content_type'].to_s,
        unlock_at:      body['unlock_at'].nil? ? nil : Time.parse(body['unlock_at']).utc.strftime(TIME_FORMAT).to_s,
        lock_at:        body['lock_at'].nil? ? nil : Time.parse(body['lock_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:     body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
      }

    when 'attachment_deleted'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        attachment_id:  body['attachment_id'].nil? ? nil : body['attachment_id'].to_i,
        user_id:        body['user_id'].nil? ? nil : body['user_id'].to_i,
        display_name:   body['display_name'].nil? ? nil : body['display_name'].to_s,
        filename:       body['filename'].nil? ? nil : body['filename'].to_s,
        folder_id:      body['folder_id'].nil? ? nil : body['folder_id'].to_i,
        context_type:   body['context_type'].nil? ? nil : body['context_type'].to_s,
        context_id:     body['context_id'].nil? ? nil : body['context_id'].to_i,
        content_type:   body['content_type'].nil? ? nil : body['content_type'].to_s,
        unlock_at:      body['unlock_at'].nil? ? nil : Time.parse(body['unlock_at']).utc.strftime(TIME_FORMAT).to_s,
        lock_at:        body['lock_at'].nil? ? nil : Time.parse(body['lock_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:     body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
      }

    when 'attachment_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        attachment_id:    body['attachment_id'].nil? ? nil : body['attachment_id'].to_i,
        user_id:          body['user_id'].nil? ? nil : body['user_id'].to_i,
        display_name:     body['display_name'].nil? ? nil : body['display_name'].to_s,
        old_display_name: body['old_display_name'].nil? ? nil : body['old_display_name'].to_s,
        folder_id:        body['folder_id'].nil? ? nil : body['folder_id'].to_i,
        filename:         body['filename'].nil? ? nil : body['filename'].to_s,
        context_type:     body['context_type'].nil? ? nil : body['context_type'].to_s,
        context_id:       body['context_id'].nil? ? nil : body['context_id'].to_i,
        content_type:     body['content_type'].nil? ? nil : body['content_type'].to_s,
        unlock_at:        body['unlock_at'].nil? ? nil : Time.parse(body['unlock_at']).utc.strftime(TIME_FORMAT).to_s,
        lock_at:          body['lock_at'].nil? ? nil : Time.parse(body['lock_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:       body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
      }

    when 'content_migration_completed'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        content_migration_id: body['content_migration_id'].nil? ? nil : body['content_migration_id'].to_i,
        context_id:           body['context_id'].nil? ? nil : body['context_id'].to_i,
        context_type:         body['context_type'].nil? ? nil : body['context_type'].to_s,
        lti_context_id:       body['lti_context_id'].nil? ? nil : body['lti_context_id'].to_s,
        context_uuid:         body['context_uuid'].nil? ? nil : body['context_uuid'].to_s,
        import_quizzes_next:  body['import_quizzes_next'].nil? ? nil : body['import_quizzes_next'].to_s,
      }

    when 'course_completed'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        # body progress
        requirement_count:            body['progress']['requirement_count'].nil? ? nil : body['progress']['requirement_count'].to_i,
        requirement_completed_count:  body['progress']['requirement_completed_count'].nil? ? nil : body['progress']['requirement_completed_count'].to_i,
        next_requirement_url:         body['progress']['next_requirement_url'].nil? ? nil : body['progress']['next_requirement_url'].to_s,
        completed_at:                 body['progress']['completed_at'].nil? ? nil : Time.parse(body['progress']['completed_at']).utc.strftime(TIME_FORMAT).to_s,
        # body user
        user_id:                      body['user']['id'].nil? ? nil : body['user']['id'].to_i,
        user_name:                    body['user']['name'].nil? ? nil : body['user']['name'].to_s,
        user_email:                   body['user']['email'].nil? ? nil : body['user']['email'].to_s,
        # body course
        course_id:                    body['course']['id'].nil? ? nil : body['course']['id'].to_i,
        course_name:                  body['course']['name'].nil? ? nil : body['course']['name'].to_s,
      }

    when 'course_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        course_id:      body['course_id'].nil? ? nil : body['course_id'].to_i,
        uuid:           body['uuid'].nil? ? nil : body['uuid'].to_s,
        account_id:     body['account_id'].nil? ? nil : body['account_id'].to_i,
        name:           body['name'].nil? ? nil : body['name'].to_s,
        created_at:     body['created_at'].nil? ? nil : Time.parse(body['created_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:     body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
        workflow_state: body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
      }

    when 'course_section_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        course_section_id:                      body['course_section_id'].nil? ? nil : body['course_section_id'].to_i,
        sis_source_id:                          body['sis_source_id'].nil? ? nil : body['sis_source_id'].to_s,
        sis_batch_id:                           body['sis_batch_id'].nil? ? nil : body['sis_batch_id'].to_s,
        course_id:                              body['course_id'].nil? ? nil : body['course_id'].to_i,
        enrollment_term_id:                     body['enrollment_term_id'].nil? ? nil : body['enrollment_term_id'].to_s,
        name:                                   body['name'].nil? ? nil : body['name'].to_s,
        default_section:                        body['default_section'].nil? ? nil : body['default_section'].to_s,
        accepting_enrollments:                  body['accepting_enrollments'].nil? ? nil : body['accepting_enrollments'].to_s,
        can_manually_enroll:                    body['can_manually_enroll'].nil? ? nil : body['can_manually_enroll'].to_s,
        start_at:                               body['start_at'].nil? ? nil : Time.parse(body['start_at']).utc.strftime(TIME_FORMAT).to_s,
        end_at:                                 body['end_at'].nil? ? nil : Time.parse(body['end_at']).utc.strftime(TIME_FORMAT).to_s,
        workflow_state:                         body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
        restrict_enrollments_to_section_dates:  body['restrict_enrollments_to_section_dates'].nil? ? nil : body['restrict_enrollments_to_section_dates'].to_s,
        nonxlist_course_id:                     body['nonxlist_course_id'].nil? ? nil : body['nonxlist_course_id'].to_s,
        stuck_sis_fields:                       body['stuck_sis_fields'].length == 0 ? nil : body['stuck_sis_fields'].join(','),
        integration_id:                         body['integration_id'].nil? ? nil : body['integration_id'].to_s,
      }

    when 'course_section_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        course_section_id:                      body['course_section_id'].nil? ? nil : body['course_section_id'].to_i,
        sis_source_id:                          body['sis_source_id'].nil? ? nil : body['sis_source_id'].to_s,
        sis_batch_id:                           body['sis_batch_id'].nil? ? nil : body['sis_batch_id'].to_s,
        course_id:                              body['course_id'].nil? ? nil : body['course_id'].to_i,
        enrollment_term_id:                     body['enrollment_term_id'].nil? ? nil : body['enrollment_term_id'].to_s,
        name:                                   body['name'].nil? ? nil : body['name'].to_s,
        default_section:                        body['default_section'].nil? ? nil : body['default_section'].to_s,
        accepting_enrollments:                  body['accepting_enrollments'].nil? ? nil : body['accepting_enrollments'].to_s,
        can_manually_enroll:                    body['can_manually_enroll'].nil? ? nil : body['can_manually_enroll'].to_s,
        start_at:                               body['start_at'].nil? ? nil : Time.parse(body['start_at']).utc.strftime(TIME_FORMAT).to_s,
        end_at:                                 body['end_at'].nil? ? nil : Time.parse(body['end_at']).utc.strftime(TIME_FORMAT).to_s,
        workflow_state:                         body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
        restrict_enrollments_to_section_dates:  body['restrict_enrollments_to_section_dates'].nil? ? nil : body['restrict_enrollments_to_section_dates'].to_s,
        nonxlist_course_id:                     body['nonxlist_course_id'].nil? ? nil : body['nonxlist_course_id'].to_s,
        stuck_sis_fields:                       body['stuck_sis_fields'].length == 0 ? nil : body['stuck_sis_fields'].join(','),
        integration_id:                         body['integration_id'].nil? ? nil : body['integration_id'].to_s,
      }

    when 'course_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        course_id:      body['course_id'].nil? ? nil : body['course_id'].to_i,
        account_id:     body['account_id'].nil? ? nil : body['account_id'].to_i,
        uuid:           body['uuid'].nil? ? nil : body['uuid'].to_s,
        name:           body['name'].nil? ? nil : body['name'].to_s,
        created_at:     body['created_at'].nil? ? nil : Time.parse(body['created_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:     body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
        workflow_state: body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
      }

    when 'discussion_entry_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        discussion_entry_id:                body['discussion_entry_id'].nil? ? nil : body['discussion_entry_id'].to_i,
        parent_discussion_entry_id:         body['parent_discussion_entry_id'].nil? ? nil : body['parent_discussion_entry_id'].to_i,
        parent_discussion_entry_author_id:  body['parent_discussion_entry_author_id'].nil? ? nil : body['parent_discussion_entry_author_id'].to_i,
        discussion_topic_id:                body['discussion_topic_id'].nil? ? nil : body['discussion_topic_id'].to_i,
        text:                               body['text'].nil? ? nil : body['text'].to_s,
      }

    when 'discussion_topic_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        discussion_topic_id:  body['discussion_topic_id'].nil? ? nil : body['discussion_topic_id'].to_i,
        is_announcement:      body['is_announcement'].nil? ? nil : body['is_announcement'].to_s,
        title:                body['title'].nil? ? nil : body['title'].to_s,
        body:                 body['body'].nil? ? nil : body['body'].to_s,
        assignment_id:        body['body'].nil? ? nil : body['body'].to_i, 
        context_id:           body['context_id'].nil? ? nil : body['context_id'].to_i,
        context_type:         body['context_type'].nil? ? nil : body['context_type'].to_s,
        workflow_state:       body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
        lock_at:              body['lock_at'].nil? ? nil : body['lock_at'].to_s,
        updated_at:           body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
      }
    
    when 'discussion_topic_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        discussion_topic_id:  body['discussion_topic_id'].nil? ? nil : body['discussion_topic_id'].to_i,
        is_announcement:      body['is_announcement'].nil? ? nil : body['is_announcement'].to_s,
        title:                body['title'].nil? ? nil : body['title'].to_s,
        body:                 body['body'].nil? ? nil : body['body'].to_s,
        assignment_id:        body['body'].nil? ? nil : body['body'].to_i, 
        context_id:           body['context_id'].nil? ? nil : body['context_id'].to_i,
        context_type:         body['context_type'].nil? ? nil : body['context_type'].to_s,
        workflow_state:       body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
        lock_at:              body['lock_at'].nil? ? nil : body['lock_at'].to_s,
        updated_at:           body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
      }

    when 'enrollment_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        enrollment_id:                       body['enrollment_id'].nil? ? nil : body['enrollment_id'].to_i,
        course_id:                           body['course_id'].nil? ? nil : body['course_id'].to_i,
        user_id:                             body['user_id'].nil? ? nil : body['user_id'].to_i,
        user_name:                           body['user_name'].nil? ? nil : body['user_name'].to_s,
        type:                                body['type'].nil? ? nil : body['type'].to_s,
        created_at:                          body['created_at'].nil? ? nil : Time.parse(body['created_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:                          body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
        limit_privileges_to_course_section:  body['limit_privileges_to_course_section'].nil? ? nil : body['limit_privileges_to_course_section'].to_s,
        course_section_id:                   body['course_section_id'].nil? ? nil : body['course_section_id'].to_i,
        associated_user_id:                  body['associated_user_id'].nil? ? nil : body['associated_user_id'].to_i,
        workflow_state:                      body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
      }

    when 'enrollment_state_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        enrollment_id:          body['enrollment_id'].nil? ? nil : body['enrollment_id'].to_i,
        state:                  body['state'].nil? ? nil : body['state'].to_s,
        state_started_at:       body['state_started_at'].nil? ? nil : Time.parse(body['state_started_at']).utc.strftime(TIME_FORMAT).to_s,
        state_is_current:       body['state_is_current'].nil? ? nil : body['state_is_current'].to_s,
        state_valid_until:      body['state_valid_until'].nil? ? nil : Time.parse(body['state_valid_until']).utc.strftime(TIME_FORMAT).to_s,
        restricted_access:      body['restricted_access'].nil? ? nil : body['restricted_access'].to_s,
        access_is_current:      body['access_is_current'].nil? ? nil : body['access_is_current'].to_s,
        state_invalidated_at:   body['state_invalidated_at'].nil? ? nil : Time.parse(body['state_invalidated_at']).utc.strftime(TIME_FORMAT).to_s,
        state_recalculated_at:  body['state_recalculated_at'].nil? ? nil : Time.parse(body['state_recalculated_at']).utc.strftime(TIME_FORMAT).to_s,
        access_invalidated_at:  body['access_invalidated_at'].nil? ? nil : Time.parse(body['access_invalidated_at']).utc.strftime(TIME_FORMAT).to_s,
        access_recalculated_at: body['access_recalculated_at'].nil? ? nil : Time.parse(body['access_recalculated_at']).utc.strftime(TIME_FORMAT).to_s,
      }

    when 'enrollment_state_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        enrollment_id:          body['enrollment_id'].nil? ? nil : body['enrollment_id'].to_i,
        state:                  body['state'].nil? ? nil : body['state'].to_s,
        state_started_at:       body['state_started_at'].nil? ? nil : Time.parse(body['state_started_at']).utc.strftime(TIME_FORMAT).to_s,
        state_is_current:       body['state_is_current'].nil? ? nil : body['state_is_current'].to_s,
        state_valid_until:      body['state_valid_until'].nil? ? nil : Time.parse(body['state_valid_until']).utc.strftime(TIME_FORMAT).to_s,
        restricted_access:      body['restricted_access'].nil? ? nil : body['restricted_access'].to_s,
        access_is_current:      body['access_is_current'].nil? ? nil : body['access_is_current'].to_s,
        state_invalidated_at:   body['state_invalidated_at'].nil? ? nil : Time.parse(body['state_invalidated_at']).utc.strftime(TIME_FORMAT).to_s,
        state_recalculated_at:  body['state_recalculated_at'].nil? ? nil : Time.parse(body['state_recalculated_at']).utc.strftime(TIME_FORMAT).to_s,
        access_invalidated_at:  body['access_invalidated_at'].nil? ? nil : Time.parse(body['access_invalidated_at']).utc.strftime(TIME_FORMAT).to_s,
        access_recalculated_at: body['access_recalculated_at'].nil? ? nil : Time.parse(body['access_recalculated_at']).utc.strftime(TIME_FORMAT).to_s,
      }

    when 'enrollment_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        enrollment_id:                      body['enrollment_id'].nil? ? nil : body['enrollment_id'].to_i,
        course_id:                          body['course_id'].nil? ? nil : body['course_id'].to_i,
        user_id:                            body['user_id'].nil? ? nil : body['user_id'].to_i,
        user_name:                          body['user_name'].nil? ? nil : body['user_name'].to_s,
        type:                               body['type'].nil? ? nil : body['type'].to_s,
        created_at:                         body['created_at'].nil? ? nil : Time.parse(body['created_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:                         body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
        limit_privileges_to_course_section: body['limit_privileges_to_course_section'].nil? ? nil : body['limit_privileges_to_course_section'].to_s,
        course_section_id:                  body['course_section_id'].nil? ? nil : body['course_section_id'].to_i,
        associated_user_id:                 body['associated_user_id'].nil? ? nil : body['associated_user_id'].to_i,
        workflow_state:                     body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
      }

    when 'grade_change'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        submission_id:        body['submission_id'].nil? ? nil : body['submission_id'].to_i,
        assignment_id:        body['assignment_id'].nil? ? nil : body['assignment_id'].to_i,
        grade:                body['grade'].nil? ? nil : body['grade'].to_s,
        old_grade:            body['old_grade'].nil? ? nil : body['old_grade'].to_s,
        score:                body['score'].nil? ? nil : body['score'].to_f,
        old_score:            body['old_score'].nil? ? nil : body['old_score'].to_f,
        points_possible:      body['points_possible'].nil? ? nil : body['points_possible'].to_f,
        old_points_possible:  body['old_points_possible'].nil? ? nil : body['old_points_possible'].to_f,
        grader_id:            body['grader_id'].nil? ? nil : body['grader_id'].to_i,
        student_id:           body['student_id'].nil? ? nil : body['student_id'].to_i,
        student_sis_id:       body['student_sis_id'].nil? ? nil : body['student_sis_id'].to_s,
        user_id:              body['user_id'].nil? ? nil : body['user_id'].to_i,
        grading_complete:     body['grading_complete'].nil? ? nil : body['grading_complete'].to_s,
        muted:                body['muted'].nil? ? nil : body['muted'].to_s
      }

    when 'group_category_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        group_category_id:    body['group_category_id'].nil? ? nil : body['group_category_id'].to_i,
        group_category_name:  body['group_category_name'].nil? ? nil : body['group_category_name'].to_s,
        context_id:           body['context_id'].nil? ? nil : body['context_id'].to_i,
        context_type:         body['context_type'].nil? ? nil : body['context_type'].to_s,
        group_limit:          body['group_limit'].nil? ? nil : body['group_limit'].to_i,
      }

    when 'group_category_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        group_category_id:    body['group_category_id'].nil? ? nil : body['group_category_id'].to_i,
        group_category_name:  body['group_category_name'].nil? ? nil : body['group_category_name'].to_s,
        context_id:           body['context_id'].nil? ? nil : body['context_id'].to_i,
        context_type:         body['context_type'].nil? ? nil : body['context_type'].to_s,
        group_limit:          body['group_limit'].nil? ? nil : body['group_limit'].to_i,
      }

    when 'group_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        group_category_id:    body['group_category_id'].nil? ? nil : body['group_category_id'].to_i,
        group_category_name:  body['group_category_name'].nil? ? nil : body['group_category_name'].to_s,
        group_id:             body['group_id'].nil? ? nil : body['group_id'].to_i,
        group_name:           body['group_name'].nil? ? nil : body['group_name'].to_s,
        uuid:                 body['uuid'].nil? ? nil : body['uuid'].to_s,
        context_type:         body['context_type'].nil? ? nil : body['context_type'].to_s,
        context_id:           body['context_id'].nil? ? nil : body['context_id'].to_i,
        account_id:           body['account_id'].nil? ? nil : body['account_id'].to_i,
        workflow_state:       body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
        max_membership:       body['max_membership'].nil? ? nil : body['max_membership'].to_i,
      }

    when 'group_membership_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        group_membership_id:  body['group_membership_id'].nil? ? nil : body['group_membership_id'].to_i,
        user_id:              body['user_id'].nil? ? nil : body['user_id'].to_i,
        group_id:             body['group_id'].nil? ? nil : body['group_id'].to_i,
        group_name:           body['group_name'].nil? ? nil : body['group_name'].to_s,
        group_category_id:    body['group_category_id'].nil? ? nil : body['group_category_id'].to_i,
        group_category_name:  body['group_category_name'].nil? ? nil : body['group_category_name'].to_s,
        workflow_state:       body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
      }

    when 'group_membership_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        group_membership_id:  body['group_membership_id'].nil? ? nil : body['group_membership_id'].to_i,
        user_id:              body['user_id'].nil? ? nil : body['user_id'].to_i,
        group_id:             body['group_id'].nil? ? nil : body['group_id'].to_i,
        group_name:           body['group_name'].nil? ? nil : body['group_name'].to_s,
        group_category_id:    body['group_category_id'].nil? ? nil : body['group_category_id'].to_i,
        group_category_name:  body['group_category_name'].nil? ? nil : body['group_category_name'].to_s,
        workflow_state:       body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
      }

    when 'group_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        group_category_id:    body['group_category_id'].nil? ? nil : body['group_category_id'].to_i,
        group_category_name:  body['group_category_name'].nil? ? nil : body['group_category_name'].to_s,
        group_id:             body['group_id'].nil? ? nil : body['group_id'].to_i,
        group_name:           body['group_name'].nil? ? nil : body['group_name'].to_s,
        uuid:                 body['uuid'].nil? ? nil : body['uuid'].to_s,
        context_type:         body['context_type'].nil? ? nil : body['context_type'].to_s,
        context_id:           body['context_id'].nil? ? nil : body['context_id'].to_i,
        account_id:           body['account_id'].nil? ? nil : body['account_id'].to_i,
        workflow_state:       body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
        max_membership:       body['max_membership'].nil? ? nil : body['max_membership'].to_i,
      }

    when 'logged_in'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        redirect_url: body['redirect_url'].nil? ? nil : body['redirect_url'].to_s,
      }
      
    when 'logged_out'

      metadata = metadata(event_data['metadata'])
      # body
      # body = event_data['body']
      bodydata = {}

    when 'module_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        module_id:      body['module_id'].nil? ? nil : body['module_id'].to_i,
        context_id:     body['context_id'].nil? ? nil : body['context_id'].to_i,
        context_type:   body['context_type'].nil? ? nil : body['context_type'].to_s,
        name:           body['name'].nil? ? nil : body['name'].to_s,
        position:       body['position'].nil? ? nil : body['position'].to_i,
        workflow_state: body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
      }

    when 'module_item_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        module_item_id: body['module_item_id'].nil? ? nil : body['module_item_id'].to_i,
        module_id:      body['module_id'].nil? ? nil : body['module_id'].to_i,
        context_id:     body['context_id'].nil? ? nil : body['context_id'].to_i,
        context_type:   body['context_type'].nil? ? nil : body['context_type'].to_s,
        position:       body['position'].nil? ? nil : body['position'].to_i,
        workflow_state: body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
      }

    when 'module_item_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        module_item_id: body['module_item_id'].nil? ? nil : body['module_item_id'].to_i,
        module_id:      body['module_id'].nil? ? nil : body['module_id'].to_i,
        context_id:     body['context_id'].nil? ? nil : body['context_id'].to_i,
        context_type:   body['context_type'].nil? ? nil : body['context_type'].to_s,
        position:       body['position'].nil? ? nil : body['position'].to_i,
        workflow_state: body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
      }

    when 'module_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        module_id:      body['module_id'].nil? ? nil : body['module_id'].to_i,
        context_id:     body['context_id'].nil? ? nil : body['context_id'].to_i,
        context_type:   body['context_type'].nil? ? nil : body['context_type'].to_s,
        name:           body['name'].nil? ? nil : body['name'].to_s,
        position:       body['position'].nil? ? nil : body['position'].to_i,
        workflow_state: body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
      }

    when 'plagiarism_resubmit'
      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        submission_id:      body['submission_id'].nil? ? nil : body['submission_id'].to_i,
        assignment_id:      body['assignment_id'].nil? ? nil : body['assignment_id'].to_i,
        user_id:            body['user_id'].nil? ? nil : body['user_id'].to_i,
        submitted_at:       body['submitted_at'].nil? ? nil : Time.parse(body['submitted_at']).utc.strftime(TIME_FORMAT).to_s,
        lti_user_id:        body['lti_user_id'].nil? ? nil : body['lti_user_id'].to_s,
        graded_at:          body['graded_at'].nil? ? nil : Time.parse(body['graded_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:         body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
        score:              body['score'].nil? ? nil : body['score'].to_s,
        grade:              body['grade'].nil? ? nil : body['grade'].to_s,
        submission_type:    body['submission_type'].nil? ? nil : body['submission_type'].to_s,
        body:               body['body'].nil? ? nil : body['body'].to_s,
        url:                body['url'].nil? ? nil : body['url'].to_s,
        attempt:            body['attempt'].nil? ? nil : body['attempt'].to_i,
        lti_assignment_id:  body['lti_assignment_id'].nil? ? nil : body['lti_assignment_id'].to_s,
        group_id:           body['group_id'].nil? ? nil : body['group_id'].to_i,
      }

    when 'quiz_export_complete'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        assignment_resource_link_id:  body['assignment']['resource_link_id'].nil? ? nil : body['assignment']['resource_link_id'].to_s,
        assignment_title:             body['assignment']['title'].nil? ? nil : body['assignment']['title'].to_s,
        assignment_context_title:     body['assignment']['context_title'].nil? ? nil : body['assignment']['context_title'].to_s,
        assignment_course_uuid:       body['assignment']['course_uuid'].nil? ? nil : body['assignment']['course_uuid'].to_s,
        qti_export_url:               body['qti_export']['url'].nil? ? nil : body['qti_export']['url'].to_s,
      }

    when 'quiz_submitted'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        submission_id:  body['submission_id'].nil? ? nil : body['submission_id'].to_i,
        quiz_id:        body['quiz_id'].nil? ? nil : body['quiz_id'].to_i,
      }

    when 'quizzes.item_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']

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
        id:                                   body['id'].nil? ? nil : body['id'].to_i,
        title:                                body['title'].nil? ? nil : body['title'].to_s,
        label:                                body['label'].nil? ? nil : body['label'].to_s,
        item_body:                            body['item_body'].nil? ? nil : body['item_body'].to_s,
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
        user_response_type:                   body['user_response_type'].nil? ? nil : body['user_response_type'].to_s,
        outcome_alignment_set_guid:           body['outcome_alignment_set_guid'].nil? ? nil : body['outcome_alignment_set_guid'].to_s,
        scoring_data:                         scoring_data,
        scoring_algorithm:                    body['scoring_algorithm'].nil? ? nil : body['scoring_algorithm'].to_s,
      }

    when 'quizzes.item_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']

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
        id:                                   body['id'].nil? ? nil : body['id'].to_i,
        title:                                body['title'].nil? ? nil : body['title'].to_s,
        label:                                body['label'].nil? ? nil : body['label'].to_s,
        item_body:                            body['item_body'].nil? ? nil : body['item_body'].to_s,
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
        user_response_type:                   body['user_response_type'].nil? ? nil : body['user_response_type'].to_s,
        outcome_alignment_set_guid:           body['outcome_alignment_set_guid'].nil? ? nil : body['outcome_alignment_set_guid'].to_s,
        scoring_data:                         scoring_data,
        scoring_algorithm:                    body['scoring_algorithm'].nil? ? nil : body['scoring_algorithm'].to_s,
      }

    when 'quizzes-lti.grade_changed'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        user_uuid:      body['user_uuid'].nil? ? nil : body['user_uuid'].to_s,
        quiz_id:        body['quiz_id'].nil? ? nil : body['quiz_id'].to_i,
        score_to_keep:  body['score_to_keep'].nil? ? nil : body['score_to_keep'].to_s,
      }

    when 'quizzes_next_quiz_duplicated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        new_assignment_id:          body['new_assignment_id'].nil? ? nil : body['new_assignment_id'].to_i,
        original_course_uuid:       body['original_course_uuid'].nil? ? nil : body['original_course_uuid'].to_s,
        original_resource_link_id:  body['original_resource_link_id'].nil? ? nil : body['original_resource_link_id'].to_s,
        new_course_uuid:            body['new_course_uuid'].nil? ? nil : body['new_course_uuid'].to_s,
        new_course_id:              body['new_course_id'].nil? ? nil : body['new_course_id'].to_s,
        new_resource_link_id:       body['new_resource_link_id'].nil? ? nil : body['new_resource_link_id'].to_s,
      }

    when 'quizzes.qti_import_completed'
      
      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        quiz_id:  body['quiz_id'].nil? ? nil : body['quiz_id'].to_i,
        success:  body['success'].nil? ? nil : body['success'].to_s,
      }

    when 'quizzes.quiz_clone_job_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        id:               body['id'].nil? ? nil : body['id'].to_s,
        original_quiz_id: body['original_quiz_id'].nil? ? nil : body['original_quiz_id'].to_s,
        status:           body['status'].nil? ? nil : body['status'].to_s,
      }

    when 'quizzes.quiz_clone_job_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        id:               body['id'].nil? ? nil : body['id'].to_s,
        original_quiz_id: body['original_quiz_id'].nil? ? nil : body['original_quiz_id'].to_s,
        status:           body['status'].nil? ? nil : body['status'].to_s,
        cloned_quiz_id:   body['cloned_quiz_id'].nil? ? nil : body['cloned_quiz_id'].to_s,
      }

    when 'quizzes.quiz_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        id:                             body['id'].nil? ? nil : body['id'].to_i,
        title:                          body['title'].nil? ? nil : body['title'].to_s,
        instructions:                   body['instructions'].nil? ? nil : body['instructions'].to_s,
        context_id:                     body['context_id'].nil? ? nil : body['context_id'].to_i,
        owner:                          body['owner'].nil? ? nil : body['owner'].to_s,
        has_time_limit:                 body['has_time_limit'].nil? ? nil : body['has_time_limit'].to_s,
        due_at:                         body['due_at'].nil? ? nil : Time.parse(body['due_at']).utc.strftime(TIME_FORMAT).to_s,
        lock_at:                        body['lock_at'].nil? ? nil : Time.parse(body['lock_at']).utc.strftime(TIME_FORMAT).to_s,
        session_time_limit_in_seconds:  body['session_time_limit_in_seconds'].nil? ? nil : body['session_time_limit_in_seconds'].to_i,
        shuffle_questions:              body['shuffle_questions'].nil? ? nil : body['shuffle_questions'].to_s,
        shuffle_answers:                body['shuffle_answers'].nil? ? nil : body['shuffle_answers'].to_s,
        status:                         body['status'].nil? ? nil : body['status'].to_s,
        outcome_alignment_set_guid:     body['outcome_alignment_set_guid'].nil? ? nil : body['outcome_alignment_set_guid'].to_s,       
      }

    when 'quizzes.quiz_graded'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        quiz_session_id:        body['quiz_session_id'].nil? ? nil : body['quiz_session_id'].to_i,
        quiz_session_result_id: body['quiz_session_result_id'].nil? ? nil : body['quiz_session_result_id'].to_i,
        grader_id:              body['grader_id'].nil? ? nil : body['grader_id'].to_i,
        grading_method:         body['grading_method'].nil? ? nil : body['grading_method'].to_s,
        status:                 body['status'].nil? ? nil : body['status'].to_s,
        score:                  body['score'].nil? ? nil : body['score'].to_f,
        fudge_points:           body['fudge_points'].nil? ? nil : body['fudge_points'].to_s,
        points_possible:        body['points_possible'].nil? ? nil : body['points_possible'].to_f,
        percentage:             body['percentage'].nil? ? nil : body['percentage'].to_f,
        created_at:             body['created_at'].nil? ? nil : Time.parse(body['created_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:             body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
      }

    # when 'quizzes.quiz_session_graded'
    when 'quizzes.quiz_session_submitted'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        accepted_student_access_code_at:    body['accepted_student_access_code_at'].nil? ? nil : Time.parse(body['accepted_student_access_code_at']).utc.strftime(TIME_FORMAT).to_s,
        allow_backtracking:                 body['allow_backtracking'].nil? ? nil : body['allow_backtracking'].to_s,
        attempt:                            body['attempt'].nil? ? nil : body['attempt'].to_i,
        authoritative_result_id:            body['authoritative_result_id'].nil? ? nil : body['authoritative_result_id'].to_i,
        created_at:                         body['created_at'].nil? ? nil : Time.parse(body['created_at']).utc.strftime(TIME_FORMAT).to_s,
        end_at:                             body['end_at'].nil? ? nil : Time.parse(body['end_at']).utc.strftime(TIME_FORMAT).to_s,
        grade_passback_guid:                body['grade_passback_guid'].nil? ? nil : body['grade_passback_guid'].to_s,
        graded_url:                         body['graded_url'].nil? ? nil : body['graded_url'].to_s,
        id:                                 body['id'].nil? ? nil : body['id'].to_i,
        invalidated_student_access_code_at: body['invalidated_student_access_code_at'].nil? ? nil : Time.parse(body['invalidated_student_access_code_at']).utc.strftime(TIME_FORMAT).to_s,
        one_at_a_time_type:                 body['one_at_a_time_type'].nil? ? nil : body['one_at_a_time_type'].to_s,
        passback_url:                       body['passback_url'].nil? ? nil : body['passback_url'].to_s,
        points_possible:                    body['points_possible'].nil? ? nil : body['points_possible'].to_f,
        quiz_id:                            body['quiz_id'].nil? ? nil : body['quiz_id'].to_i,
        session_items_count:                body['session_items_count'].nil? ? nil : body['session_items_count'].to_i,
        start_at:                           body['start_at'].nil? ? nil : Time.parse(body['start_at']).utc.strftime(TIME_FORMAT).to_s,
        status:                             body['status'].nil? ? nil : body['status'].to_s,
        submitted_at:                       body['submitted_at'].nil? ? nil : Time.parse(body['submitted_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:                         body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
      }

    when 'quizzes.quiz_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        id:                             body['id'].nil? ? nil : body['id'].to_i,
        title:                          body['title'].nil? ? nil : body['title'].to_s,
        instructions:                   body['instructions'].nil? ? nil : body['instructions'].to_s,
        context_id:                     body['context_id'].nil? ? nil : body['context_id'].to_i,
        owner:                          body['owner'].nil? ? nil : body['owner'].to_s,
        has_time_limit:                 body['has_time_limit'].nil? ? nil : body['has_time_limit'].to_s,
        due_at:                         body['due_at'].nil? ? nil : Time.parse(body['due_at']).utc.strftime(TIME_FORMAT).to_s,
        lock_at:                        body['lock_at'].nil? ? nil : Time.parse(body['lock_at']).utc.strftime(TIME_FORMAT).to_s,
        session_time_limit_in_seconds:  body['session_time_limit_in_seconds'].nil? ? nil : body['session_time_limit_in_seconds'].to_i,
        shuffle_answers:                body['shuffle_answers'].nil? ? nil : body['shuffle_answers'].to_s,
        shuffle_questions:              body['shuffle_questions'].nil? ? nil : body['shuffle_questions'].to_s,
        status:                         body['status'].nil? ? nil : body['status'].to_s,
        outcome_alignment_set_guid:     body['outcome_alignment_set_guid'].nil? ? nil : body['outcome_alignment_set_guid'].to_s,
      }

    when 'submission_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        submission_id:      body['submission_id'].nil? ? nil : body['submission_id'].to_i,
        assignment_id:      body['assignment_id'].nil? ? nil : body['assignment_id'].to_i,
        user_id:            body['user_id'].nil? ? nil : body['user_id'].to_i,
        submitted_at:       body['submitted_at'].nil? ? nil : Time.parse(body['submitted_at']).utc.strftime(TIME_FORMAT).to_s,
        lti_user_id:        body['lti_user_id'].nil? ? nil : body['lti_user_id'].to_s,
        graded_at:          body['graded_at'].nil? ? nil : Time.parse(body['graded_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:         body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
        score:              body['score'].nil? ? nil : body['score'].to_f,
        grade:              body['grade'].nil? ? nil : body['grade'].to_s,
        submission_type:    body['submission_type'].nil? ? nil : body['submission_type'].to_s,
        body:               body['body'].nil? ? nil : body['body'].to_s,
        url:                body['url'].nil? ? nil : body['url'].to_s,
        attempt:            body['attempt'].nil? ? nil : body['attempt'].to_i,
        lti_assignment_id:  body['lti_assignment_id'].nil? ? nil : body['lti_assignment_id'].to_s,
        group_id:           body['group_id'].nil? ? nil : body['group_id'].to_i,
      }

    when 'submission_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        submission_id:      body['submission_id'].nil? ? nil : body['submission_id'].to_i,
        assignment_id:      body['assignment_id'].nil? ? nil : body['assignment_id'].to_i,
        user_id:            body['user_id'].nil? ? nil : body['user_id'].to_i,
        submitted_at:       body['submitted_at'].nil? ? nil : Time.parse(body['submitted_at']).utc.strftime(TIME_FORMAT).to_s,
        lti_user_id:        body['lti_user_id'].nil? ? nil : body['lti_user_id'].to_s,
        lti_assignment_id:  body['lti_assignment_id'].nil? ? nil : body['lti_assignment_id'].to_s,
        graded_at:          body['graded_at'].nil? ? nil : Time.parse(body['graded_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:         body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
        score:              body['score'].nil? ? nil : body['score'].to_f,
        grade:              body['grade'].nil? ? nil : body['grade'].to_s,
        submission_type:    body['submission_type'].nil? ? nil : body['submission_type'].to_s,
        body:               body['body'].nil? ? nil : body['body'].to_s,
        url:                body['url'].nil? ? nil : body['url'].to_s,
        attempt:            body['attempt'].nil? ? nil : body['attempt'].to_i,
        group_id:           body['group_id'].nil? ? nil : body['group_id'].to_i,
      }

    when 'syllabus_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        course_id:          body['course_id'].nil? ? nil : body['course_id'].to_i,
        syllabus_body:      body['syllabus_body'].nil? ? nil : body['syllabus_body'].to_s,
        old_syllabus_body:  body['old_syllabus_body'].nil? ? nil : body['old_syllabus_body'].to_s,
      }

    when 'user_account_association_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        user_id:      body['user_id'].nil? ? nil : body['user_id'].to_i,
        account_id:   body['account_id'].nil? ? nil : body['account_id'].to_i,
        account_uuid: body['account_uuid'].nil? ? nil : body['account_uuid'].to_s,
        created_at:   body['created_at'].nil? ? nil : Time.parse(body['created_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:   body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
        is_admin:     body['is_admin'].nil? ? nil : body['is_admin'].to_s,
      }

    when 'user_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        user_id:        body['user_id'].nil? ? nil : body['user_id'].to_i,
        uuid:           body['uuid'].nil? ? nil : body['uuid'].to_s,
        name:           body['name'].nil? ? nil : body['name'].to_s,
        short_name:     body['short_name'].nil? ? nil : body['short_name'].to_s,
        workflow_state: body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
        created_at:     body['created_at'].nil? ? nil : Time.parse(body['created_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:     body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
      }

    when 'user_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        user_id:        body['user_id'].nil? ? nil : body['user_id'].to_i,
        uuid:           body['uuid'].nil? ? nil : body['uuid'].to_s,
        name:           body['name'].nil? ? nil : body['name'].to_s,
        short_name:     body['short_name'].nil? ? nil : body['short_name'].to_s,
        workflow_state: body['workflow_state'].nil? ? nil : body['workflow_state'].to_s,
        created_at:     body['created_at'].nil? ? nil : Time.parse(body['created_at']).utc.strftime(TIME_FORMAT).to_s,
        updated_at:     body['updated_at'].nil? ? nil : Time.parse(body['updated_at']).utc.strftime(TIME_FORMAT).to_s,
      }

    when 'wiki_page_created'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        wiki_page_id: body['wiki_page_id'].nil? ? nil : body['wiki_page_id'].to_i,
        title:        body['title'].nil? ? nil : body['title'].to_s,
        body:         body['body'].nil? ? nil : body['body'].to_s,
      }

    when 'wiki_page_deleted'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        wiki_page_id: body['wiki_page_id'].nil? ? nil : body['wiki_page_id'].to_i,
        title:        body['title'].nil? ? nil : body['title'].to_s,
      }

    when 'wiki_page_updated'

      metadata = metadata(event_data['metadata'])
      body = event_data['body']
      bodydata = {
        wiki_page_id: body['wiki_page_id'].nil? ? nil : body['wiki_page_id'].to_i,
        title:        body['title'].nil? ? nil : body['title'].to_s,
        body:         body['body'].nil? ? nil : body['body'].to_s,
        old_title:    body['old_title'].nil? ? nil : body['old_title'].to_s,
        old_body:     body['old_body'].nil? ? nil : body['old_body'].to_s,
      }
    
    # catch and save events, we don't have configured or we aren't expecting
    else
      collect_unknown(event_name, event_data)
      # return if the message cannot be prepped for import
      return
    end

    # merge metadata and bodydata, prevent duplicate fields
    import_data = metadata.merge!(bodydata)
    # import to db
    import(event_name, event_time, event_data, import_data, false)
    # check if we missed any new data
    bodycount(event_data, bodydata)
  end
end