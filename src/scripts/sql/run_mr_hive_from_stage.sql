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
STORED AS PARQUET;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.compress.output=true;
-- SET io.seqfile.compression.type=BLOCK;
-- NONE/RECORD/BLOCK (see below)
SET hive.exec.max.dynamic.partitions.pernode=20000;
set hive.exec.max.dynamic.partitions=6000;
SET hive.exec.max.created.files=100000;
SET mapred.job.reduce.memory.mb=2048;
SET mapred.child.java.opts='-Xmx2048m';
--SET mapreduce.map.java.opts='-Xmx2048m';
--SET mapreduce.reduce.java.opts='-Xmx2048m';
SET parquet.compression=SNAPPY;
insert into table rand_netflow_snappy_secs
PARTITION (dt, hour, minute, second)
select StartTime, Dur, Proto, SrcAddr, Dir, DstAddr, Dport, State, sTos, dTos, TotPkts, TotBytes, Label, dt, hour, minute, second from rand_netflow_snappy_sec_stage;
--select split(starttime, '[ ]')[1], sourceip, split(starttime, '[ ]')[0], split(starttime, '[:]')[0], split(starttime, '[:]')[1], split(split(starttime, '[:]')[2], '[.]')[0] from staged_netflow_records