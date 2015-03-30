use faganp;
drop table public_netflow_botnet_44_nocomp_test;
Create table public_netflow_botnet_44_nocomp_test
(StartTime timestamp,
Dur string,
Proto string,
SrcAddr string,
Sport smallint,
Dir string,
DstAddr string,
Dport tinyint,
State string,
sTos bigint,
dTos bigint,
TotPkts tinyint,
TotBytes tinyint,
SrcBytes tinyint,
Label string
)
partitioned by (dt string, hour tinyint, minute tinyint)
STORED AS PARQUET;
