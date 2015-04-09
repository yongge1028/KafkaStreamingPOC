create database if not exists faganpe;
use faganpe;
drop table rand_netflow_snappy_secs;
Create table rand_netflow_snappy_secs
(StartTime timestamp,
Dur float,
Proto string,
SrcAddr string,
Dir string,
DstAddr string,
Dport int,
State string,
sTos tinyint,
dTos tinyint,
TotPkts int,
TotBytes int,
Label string
)
partitioned by (dt string, hour tinyint, minute tinyint, second tinyint)
location '/user/faganpe/randomNetflow/output-random-netflow/parquetDataReal'
STORED AS PARQUET;
