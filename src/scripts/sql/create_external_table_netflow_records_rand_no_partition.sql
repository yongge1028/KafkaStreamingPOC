set hive.mapred.supports.subdirectories=true;
set mapred.input.dir.recursive=true;

create database if not exists cloudera;

use cloudera;

drop table if exists staged_rand_netflow;

create external table staged_rand_netflow (
StartTime string,
Dur float,
Proto string,
SrcAddr string,
Dir string,
DstAddr string,
Dport bigint,
State string,
sTos bigint,
dTos tinyint,
TotPkts smallint,
TotBytes tinyint,
Label string
)
-- partitioned by (runNum int)
row format delimited fields terminated by ','
location '/user/cloudera/randomNetflow/';

set hive.mapred.supports.subdirectories=true;
set mapred.input.dir.recursive=true;
CREATE EXTERNAL TABLE `netflowbigger`(
	  `sensor_id` tinyint,
	  `ts` string,
	  `te` string,
	  `duration` bigint,
	  `src_ip` string,
	  `src_port` smallint,
	  `dest_ip` string,
	  `dest_port` smallint,
	  `protocol` tinyint,
	  `ip_version` tinyint,
	  `packets` smallint,
	  `bytes` int,
	  `tcp_flag_a` boolean,
	  `tcp_flag_s` boolean,
	  `tcp_flag_f` boolean,
	  `tcp_flag_r` boolean,
	  `tcp_flag_p` boolean,
	  `tcp_flag_u` boolean,
	  `tos` tinyint,
	  `reason_for_flow` string,
	  `sensor_site` string,
	  `sensor_org_name` string,
	  `sensor_org_sector` string,
	  `sensor_org_type` string,
	  `sensor_priority` tinyint,
	  `sensor_country` string,
	  `geoip_src_country` string,
	  `geoip_src_subdivisions` string,
	  `geoip_src_city` string,
	  `geoip_src_lat` float,
	  `geoip_src_long` float,
	  `geoip_src_isp_org` string,
	  `geoip_src_as` string,
	  `geoip_src_as_org` smallint,
	  `geoip_dst_country` string,
	  `geoip_dst_subdivisions` string,
	  `geoip_dst_city` string,
	  `geoip_dst_lat` float,
	  `geoip_dst_long` float,
	  `geoip_dst_isp_org` string,
	  `geoip_dst_as` string,
	  `geoip_dst_as_org` tinyint,
	  `port_src_well_known_service` string,
	  `port_dst_well_known_service` string,
	  `asset_src_site` string,
	  `asset_src_org_name` string,
	  `asset_src_org_sector` string,
	  `asset_src_org_type` string,
	  `asset_src_priority` tinyint,
	  `asset_src_country` string,
	  `asset_dst_site` string,
	  `asset_dst_org_name` string,
	  `asset_dst_org_sector` string,
	  `asset_dst_org_type` string,
	  `asset_dst_priority` string,
	  `asset_dst_country` string,
	  `threat_src_type` string,
	  `threat_src_attacker` string,
	  `threat_src_malware` string,
	  `threat_src_campaign` string,
	  `threat_src_infrastructure` string,
	  `threat_dst_type` string,
	  `threat_dst_attacker` string,
	  `threat_dst_malware` string,
	  `threat_dst_campaign` string,
	  `threat_dst_infrastructure` string,
	  `yyyy` smallint,
	  `mm` tinyint,
	  `dd` tinyint,
	  `hh` tinyint,
	  `mi` tinyint)
ROW FORMAT DELIMITED
	  FIELDS TERMINATED BY ','
	STORED AS INPUTFORMAT
	  'org.apache.hadoop.mapred.TextInputFormat'
	OUTPUTFORMAT
	  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
	LOCATION
	  'hdfs://vm-cluster-node1:8020/user/admin/randomNetflow2'