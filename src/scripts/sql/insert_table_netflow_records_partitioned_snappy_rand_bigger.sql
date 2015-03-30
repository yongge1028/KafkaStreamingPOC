use faganpe;
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
insert into table rand_netflow_snappy_bigger
PARTITION (dt, hour, minute)
select split(StartTime, '[ ]')[1], Dur, Proto, SrcAddr, Dir, DstAddr, Dport, State, sTos, dTos, TotPkts, TotBytes, Label, split(starttime, '[ ]')[0], split(split(starttime, '[ ]')[1], '[:]')[0], split(split(starttime, '[ ]')[1], '[:]')[1] from staged_rand_netflow;
--select split(starttime, '[ ]')[1], sourceip, split(starttime, '[ ]')[0], split(starttime, '[:]')[0], split(starttime, '[:]')[1], split(split(starttime, '[:]')[2], '[.]')[0] from staged_netflow_records
