package Utils

/**
 * Created by faganpe on 19/09/2015.
 */

// note case class cannot have more than 22 parameters

//case class NetFlowDef (sensor_id: String, ts: String, te: String, duration: String, src_ip: String, src_port: String, dest_ip: String,
//                       dest_port: String, protocol: String, ip_version: String, packets: String, bytes: String, tcp_flag_a: String,
//                       tcp_flag_s: String, tcp_flag_f: String, tcp_flag_r: String, tcp_flag_p: String, tcp_flag_u: String, tos: String,
//                       reason_for_flow: String, sensor_site: String, sensor_org_name: String, sensor_org_sector: String,
//                       sensor_org_type: String, sensor_priority: String, sensor_country: String, geoip_src_country: String,
//                       geoip_src_subdivisions: String, geoip_src_city: String, geoip_src_lat: String, geoip_src_long: String,
//                       geoip_src_isp_org: String, geoip_src_as: String, geoip_src_as_org: String, geoip_dst_country: String,
//                       geoip_dst_subdivisions: String, geoip_dst_city: String, geoip_dst_lat: String, geoip_dst_long: String,
//                       geoip_dst_isp_org: String, geoip_dst_as: String, geoip_dst_as_org: String,
//                       port_src_well_known_service: String, port_dst_well_known_service: String, asset_src_site: String,
//                       asset_src_org_name: String, asset_src_org_sector: String, asset_src_org_type: String,
//                       asset_src_priority: String, asset_src_country: String, asset_dst_site: String, asset_dst_org_name: String,
//                       asset_dst_org_sector: String, asset_dst_org_type: String, asset_dst_priority: String, asset_dst_country: String,
//                       threat_src_type: String, threat_src_attacker: String, threat_src_malware: String, threat_src_campaign: String,
//                       threat_src_infrastructure: String, threat_dst_type: String, threat_dst_attacker: String, threat_dst_malware: String,
//                       threat_dst_campaign: String, threat_dst_infrastructure: String, yyyy: Int, mm: Int,dd: Int,hh: Int,mi: Int)

case class NetFlowDef (sensor_id: String, ts: String, te: String)
