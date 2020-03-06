def default_timezone(string)
  Time.parse(string).utc.strftime(TIME_FORMAT).to_s unless string.nil?
end

# flattens the nested/recursive data structure of events to underscore_notation
def _flatten(data, recursive_key = '')
  data.each_with_object({}) do |(k, v), ret|
    key = recursive_key + k.to_s
    key = key.gsub(/[^a-zA-Z]/, '_')
    if v.is_a? Hash
      ret.merge! _flatten(v, key + '_')
    elsif v.is_a? Array
      v.each do |x|
        if x.is_a? String
          ret[key] = v.join(',')
        else 
          ret.merge! _flatten(x, key + '_')
        end
      end
    else
      ret[key] = v
    end
  end
end

# reduces underscore notation, removing repeated and verbose strings
def _squish(hash)
  hash = _flatten(hash)
  hash.each_with_object({}) do |(k, v), ret|
    k = k.gsub(/extensions|com|instructure|canvas/, '').gsub(/_+/, '_').gsub(/^_/, '').downcase
    ret[k] = v
  end
end

def collect_unknown(event_name, event_data)
  puts "unexpected event: #{event_name}"
  # # send message to another queue for caching...?
  # puts 'storing event data in moo queue'
  # begin
  #   LiveEvents.perform_async(event_data, queue: "#{SQS_CFG['queues'][0]}-moo")
  # rescue => e
    #puts e
    #puts 'moo queue failed, saving payload to file'
    #puts event_data
    # write event and payload to file
    open('log/payload-cache.js', 'a') do |f|
      f << "\n//#{event_name}\n"
      f << event_data.to_json
    end
  # end
end

# counts the fields sent with caliper event to what is expected, log if something was missed
def caliper_count(event_name, sent, expected)
    
  # original hash, cloned, compact (no nil), strings, keys
  copy_sent = sent.clone.compact.stringify_keys.keys
  copy_expected = expected.clone.stringify_keys.keys
  
  # what's missing, get the difference
  missing = copy_sent - copy_expected # | copy_expected - copy_sent
  missing = missing.reject { |k| ['id'].include? k }
  if missing.size.positive?
    sample = missing.map { |k| "#{k}:::#{sent.fetch(k)}"}
    err = <<~ERRLOG
        event_name: ims_#{event_name}
        count: { sent: #{sent.keys.count}, defined: #{copy_expected.count} }
        summary: { event_name: ims_#{event_name}, undefined: #{missing.to_s.gsub('"', '')} }
        sample: { event_name: ims_#{event_name}, undefined: #{sample} }
        message: #{sent.to_json}

    ERRLOG
    # store in log file, print if interactive
    open('log/ddl-undefined.log', 'a') { |f| f << err }
    puts err if $stdout.isatty
  end
end

# counts the meta data fields sent with canvas event to what is expected, log if something was missed
def missing_meta(sent, expected)

  # original hash, cloned, compact (no nil), strings, keys
  sent_meta = sent['metadata'].clone.compact.stringify_keys.keys
  collected_meta = expected.clone.stringify_keys.keys
  
  # normalize the key names, since we've added _meta to some
  normal_meta = collected_meta.map{ |k| k.gsub(/_meta/, '')}
  
  # what's missing, get the difference
  missing = sent_meta - normal_meta | normal_meta - sent_meta

  # store in log file
  if missing.size.positive?
    sample = missing.map { |k| "#{k}:::#{sent['metadata'].fetch(k)}" }
    err = <<~ERRLOG
        event_name: live_#{sent['metadata']['event_name']}
        count: { sent: #{sent['metadata'].keys.count}, defined: #{normal_meta.count} }
        summary: { event_name: live_#{sent['metadata']['event_name']}, undefined: #{missing.to_s.gsub('"', '')} }
        sample: { event_name: live_#{sent['metadata']['event_name']}, undefined: #{sample} }
        message: #{sent.to_json}

    ERRLOG
    # store in log file, print if interactive
    open('log/ddl-undefined.log', 'a') { |f| f << err }
    puts err if $stdout.isatty
  end
end

# counts the body fields sent with canvas event to what is expected, log if something was missed
def missing_body(event_data, bodydata)

  ed = event_data.clone
  edB = ed['body']
  bd = bodydata.clone
  flag = false

  # flag if event body has more fields than we're expecting
  missing = edB.stringify_keys.keys - bd.stringify_keys.keys
  flag = true if missing.size.positive?
  
  # flag if there is a similar field in the metadata, but the value isn't the same
  differs = missing.select { |k| bd[k] != ed['metadata'][k] }
  flag = true if differs.size.positive?

  # compare data in nested events
  if flag == true
    # add condition, if any missing element in the body is a hash

    # for each body key, store a similar value with the missing prefix removed
    body_keys = _flatten(edB).keys
    compare = body_keys.map { |k| [k, k.sub(Regexp.union(missing), '').sub(/^_/, '')] }

    # for each comparison key, check it within the expected body keys
    # for each comparison set, if 1 item in the compare array is a match for an expected key, store the expected key to a new array
    # store missing keys to a different array
    found = []
    compare.clone.each do |c|
      c.each do |k|
        if bodydata.stringify_keys.keys.any? k
          found << k
          # don't compare it again
          compare.delete(c)
        end
      end
    end

    # if the compare is empty now, continue, nothing was missed
    if compare.empty?
      flag = false
    # something was missed, identify it
    else

      missing_nested = []
      compare.each do |c|
        c.each do |k|
          # if the key is not found in the expected bodydata, collect it
          missing_nested << k if !bodydata.stringify_keys.keys.any? k
        end
      end

      # get the difference in keys sent from expected
      # check each missing key and see if we're storing it's parent
      # remove any keys where we store the parent element and it's content
      key_diff = body_keys - bd.stringify_keys.keys
      missing_still = key_diff.clone
      key_diff.each do |k|
        bd.stringify_keys.keys.each do |dk|
          missing_still.delete(k) if key_diff.any? (/^#{dk}/)
        end
      end

      if missing_still.count == 0
        flag = false
      else
        missing = missing_still
        _sample = _flatten(edB)
        sample = missing.map { |k| "#{k}:::#{_sample.fetch(k)}" } 
        # puts "missing still"
        # puts missing_still.count

        # o = {
        #   expected: bd.stringify_keys.keys,
        #   sent: body_keys,
        #   compare: check,
        #   missing_nested: missing_nested,
        #   missing: missing,
        #   missing_still: missing_still
        # }
        # pp o
        # open('log/ddl-debug-missing.log', 'a') { |f| f << o }

      end
    end
  end

  if flag == true
    sample = missing.map { |k| "#{k}:::#{edB.fetch(k)}" } if sample.length == 0
    err = <<~ERRLOG
        event_name: live_#{ed['metadata']['event_name']}
        count: { sent: #{edB.keys.count}, defined: #{bd.keys.count} }
        summary: { event_name: #{ed['metadata']['event_name']}, undefined: #{missing.to_s.gsub('"', '')} }
        sample: { event_name: #{ed['metadata']['event_name']}, undefined: #{sample} }
        message: #{ed.to_json}

    ERRLOG
    # store in log file, print if interactive
    open('log/ddl-undefined.log', 'a') { |f| f << err }
    puts err if $stdout.isatty
  end
end