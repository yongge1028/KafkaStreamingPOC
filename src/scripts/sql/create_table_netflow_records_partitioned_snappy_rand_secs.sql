create database if not exists faganpe;
use faganpe;
drop table rand_netflow_snappy_secs;
Create table rand_netflow_snappy_secs
(StartTime string,
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
Label string,
Country string
)
-- partitioned by (dt string, hour tinyint, minute tinyint, seconds tinyint)
location '/user/faganpe/randomNetflow/output-random-netflow/parquetData'
STORED AS PARQUET;
