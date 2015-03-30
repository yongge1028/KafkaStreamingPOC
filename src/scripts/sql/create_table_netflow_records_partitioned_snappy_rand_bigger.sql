use faganpe;
drop table rand_netflow_snappy_bigger;
Create table rand_netflow_snappy_bigger
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
Label string
)
partitioned by (dt string, hour tinyint, minute tinyint)
STORED AS PARQUET;
