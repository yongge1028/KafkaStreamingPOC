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