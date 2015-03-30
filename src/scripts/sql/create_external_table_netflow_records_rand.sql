create database if not exists faganpe;

use faganpe;

drop table if exists staged_rand_netflow;

create external table staged_rand_netflow (
StartTime string,
Dur float,
Proto string,
SrcAddr string,
Dir string,
DstAddr string,
Dport smallint,
State string,
sTos bigint,
dTos tinyint,
TotPkts smallint,
TotBytes tinyint,
Label string,
Country string
)
row format delimited fields terminated by ','
--location '/user/faganp/spark-streaming/*/*/';
-- location '/user/faganp/test1/';
location '/user/faganpe/rand_netflow/output-random-netflow';
-- location '/user/faganp/test/';
