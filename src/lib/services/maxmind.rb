require 'maxmind/db'
require 'active_support/time_with_zone'

class MaxMindGeo

  def start
    puts 'w/MaxMindDB' if $stdout.isatty
    max_ip_lookup
  end

  def ready
    test = DB[:led_geo_maxmind].select(:ip).limit(1)
    if test.count == 1
      true
    end
  rescue => e
    if e.message.match? Regexp.union(WARN_ERRORS)
      warn e
      false
      # test = DB.create_table(:led_geo_maxmind) do
      #   # created
      #   primary_key :id, type: :Bignum
      #   String :ip, size: 39
      #   String :city, size: 255
      #   String :region_name, size: 255
      #   String :region_code, size: 255
      #   String :country_code, size: 6
      #   String :country_name, size: 84
      #   String :continent_code, size: 12
      #   String :in_eu, size: 5
      #   String :postal, size: 12
      #   Float :latitude
      #   Float :longitude
      #   String :timezone, size: 50
      #   String :utc_offset, size: 6
      #   String :country_calling_code, size: 6
      #   String :currency, size: 6
      #   String :languages, size: 32
      #   String :asn, size: 32
      #   String :org, size: 255
      # end
      # p test
    end
  end

  def select_ips
    DB[:live_stream].distinct.select(:client_ip).
      left_outer_join(:led_geo_maxmind, :ip => :client_ip).
      where{client_ip !~ nil}.where{ip =~ nil}.
      limit(20)
  rescue => e
    p e
  end

  def add_ip(max)
    DB[:led_geo_maxmind].insert(max)
    # {
    #   ip: max[:ip],
    #   city: max[:city],
    #   region_name: max[:region_name],
    #   region_code: max[:region_code],
    #   country_code: max[:country_code],
    #   country_name: max[:country_name],
    #   continent_code: max[:continent_code],
    #   in_eu: max[:in_eu],
    #   postal: max[:postal],
    #   latitude: max[:latitude],
    #   longitude: max[:longitude],
    #   timezone: max[:timezone],
    #   utc_offset: max[:utc_offset],
    #   country_calling_code: max[:country_calling_code],
    #   currency: max[:currency],
    #   languages: max[:languages],
    #   asn: max[:asn],
    #   org: max[:org],
    # })
  rescue => e
    p e
  end

  def utc_offset(tmzn)
    ActiveSupport::TimeZone.seconds_to_utc_offset(ActiveSupport::TimeZone[tmzn].utc_offset)
  end

  def max_ip_lookup
    Thread.new do
      if ready == false
        abort
      end

      # https://dev.maxmind.com/geoip/geoip2/geolite2/
      max_db = MaxMind::DB.new('src/lib/services/GeoLite2-City.mmdb', mode: MaxMind::DB::MODE_MEMORY)
      select_ips&.map(:client_ip)&.each do |client_ip|
        max = max_db.get(client_ip)
        
        if max.nil?
          puts "#{client_ip} was not found in the database"
        else
          ip_data = {
            ip:               client_ip,
            city:             max.key?('city') ? max['city']['names']['en']&.to_s : nil,
            region_name:      max.key?('subdivisions') ? max['subdivisions'][0]['names']['en']&.to_s : nil,
            region_code:      max.key?('subdivisions') ? max['subdivisions'][0]['iso_code']&.to_s : nil,
            country_code:     max['country']['iso_code']&.to_s,
            country_name:     max['country']['names']['en']&.to_s,
            continent_code:   max['continent']['code']&.to_s,
            in_eu:            max['continent']['code'] == 'EU' ? 1 : 0,
            postal:           max.key?('postal') ? max['postal']['code']&.to_s : nil,
            latitude:         max['location']['latitude'],
            longitude:        max['location']['longitude'],
            timezone:         max['location']['time_zone']&.to_s,
            utc_offset:       max['location'].key?('time_zone') ? utc_offset(max['location']['time_zone']) : nil,
            country_calling_code: nil,
            currency:         nil,
            languages:        nil,
            asn:              nil,
            org:              nil,
          }

          if $stdout.isatty
            time = DateTime.now.strftime('%Y-%m-%d %H:%M:%S.%L').to_s
            out = [
              ip_data[:country_name],
              ip_data[:region_name],
              ip_data[:city],
            ].compact.join('.').downcase
            printf("\r%s: %s\e[0J", time, "[geo] #{out}") 
          end

          add_ip(ip_data)
        end
      end
      max_db.close
      sleep 20
      max_ip_lookup
    end
  end
end
maxip = MaxMindGeo.new
maxip.start