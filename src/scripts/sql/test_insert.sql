use faganp;
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
SET parquet.compression=UNCOMPRESSED;
insert into table public_netflow_botnet_44_nocomp_test
PARTITION (dt, hour, minute)
select cast(split(StartTime, '[ ]')[1] as int), Dur, Proto, SrcAddr, Sport, Dir, DstAddr, Dport, State, sTos, dTos, TotPkts, TotBytes, SrcBytes, Label, split(starttime, '[ ]')[0], split(split(starttime, '[ ]')[1], '[:]')[0], split(split(starttime, '[ ]')[1], '[:]')[1] from staged_netflow_ctu_botnet_44 limit 100;
--select split(starttime, '[ ]')[1], sourceip, split(starttime, '[ ]')[0], split(starttime, '[:]')[0], split(starttime, '[:]')[1], split(split(starttime, '[:]')[2], '[.]')[0] from staged_netflow_records
