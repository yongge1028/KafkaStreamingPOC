curl -XPUT 'http://192.168.99.100:9200/spark/' -d '{
    "settings" : {
        "index" : {
            "number_of_shards" : 1,
            "number_of_replicas" : 0
        }
    },
    "mappings" : {
        "netflow": {
            "properties": {
              "port_src_well_known_service": {
                "type": "string"
              },
              "ts": {
                "format": "yyyy/MM/dd HH:mm:ss||yyyy/MM/dd",
                "type": "date"
              },
              "geoip_src_country": {
                "type": "string"
              },
              "asset_dst_priority": {
                "type": "string"
              },
              "threat_dst_infrastructure": {
                "type": "string"
              },
              "reason_for_flow": {
                "type": "string"
              },
              "threat_src_infrastructure": {
                "type": "string"
              },
              "te": {
                "type": "string"
              },
              "geoip_src_lat": {
                "type": "string"
              },
              "src_port": {
                "type": "integer"
              },
              "asset_src_org_type": {
                "type": "string"
              },
              "geoip_src_subdivisions": {
                "type": "string"
              },
              "threat_src_campaign": {
                "type": "string"
              },
              "threat_src_type": {
                "type": "string"
              },
              "geoip_dst_city": {
                "type": "string"
              },
              "geoip_dst_country": {
                "type": "string"
              },
              "geoip_dst_long": {
                "type": "string"
              },
              "threat_dst_campaign": {
                "type": "string"
              },
              "tcp_flag_a": {
                "type": "string"
              },
              "threat_dst_malware": {
                "type": "string"
              },
              "asset_dst_country": {
                "type": "string"
              },
              "sensor_priority": {
                "type": "string"
              },
              "sensor_id": {
                "type": "string"
              },
              "asset_src_priority": {
                "type": "string"
              },
              "dest_port": {
                "type": "string"
              },
              "asset_src_site": {
                "type": "string"
              },
              "tcp_flag_s": {
                "type": "string"
              },
              "geoip_src_as": {
                "type": "string"
              },
              "tcp_flag_r": {
                "type": "string"
              },
              "tcp_flag_p": {
                "type": "string"
              },
              "yyyy": {
                "type": "string"
              },
              "src_ip": {
                "type": "string"
              },
              "asset_dst_org_sector": {
                "type": "string"
              },
              "asset_src_org_sector": {
                "type": "string"
              },
              "sensor_country": {
                "type": "string"
              },
              "sensor_site": {
                "type": "string"
              },
              "threat_dst_type": {
                "type": "string"
              },
              "tcp_flag_f": {
                "type": "string"
              },
              "tcp_flag_u": {
                "type": "string"
              },
              "bytes": {
                "type": "string"
              },
              "packets": {
                "type": "string"
              },
              "geoip_src_city": {
                "type": "string"
              },
              "hh": {
                "type": "string"
              },
              "threat_src_malware": {
                "type": "string"
              },
              "sensor_org_type": {
                "type": "string"
              },
              "dd": {
                "type": "string"
              },
              "tos": {
                "type": "string"
              },
              "asset_dst_org_type": {
                "type": "string"
              },
              "dest_ip": {
                "type": "string"
              },
              "asset_dst_org_name": {
                "type": "string"
              },
              "geoip_src_long": {
                "type": "string"
              },
              "asset_dst_site": {
                "type": "string"
              },
              "sensor_org_name": {
                "type": "string"
              },
              "threat_src_attacker": {
                "type": "string"
              },
              "threat_dst_attacker": {
                "type": "string"
              },
              "geoip_dst_as": {
                "type": "string"
              },
              "geoip_dst_as_org": {
                "type": "string"
              },
              "sensor_org_sector": {
                "type": "string"
              },
              "mi": {
                "type": "string"
              },
              "mm": {
                "type": "string"
              },
              "protocol": {
                "type": "string"
              },
              "ESDateStr": {
                "type": "string"
              },
              "geoip_dst_lat": {
                "type": "string"
              },
              "asset_src_country": {
                "type": "string"
              },
              "geoip_dst_isp_org": {
                "type": "string"
              },
              "port_dst_well_known_service": {
                "type": "string"
              },
              "duration": {
                "type": "string"
              },
              "geoip_dst_subdivisions": {
                "type": "string"
              },
              "geoip_src_as_org": {
                "type": "string"
              },
              "geoip_src_isp_org": {
                "type": "string"
              },
              "ip_version": {
                "type": "string"
              },
              "asset_src_org_name": {
                "type": "string"
              }
            }
          }
    }
}'