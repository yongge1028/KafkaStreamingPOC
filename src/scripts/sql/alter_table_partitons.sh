#!/bin/bash

hdfs_dir="/user/faganpe/randomNetflow/output-random-netflow"
table_name="staged_rand_netflow"

echo "use faganpe;"

for dir in $(hdfs dfs -ls ${hdfs_dir} | grep -v '.Trash' | grep -v '.staging' | grep -v 'Found' | grep -v hdfs |  awk '{print $8}')
do
DT=$(echo ${dir} | awk -F '/' '{ print $(NF) }' | cut -d '=' -f 2)
echo "alter table ${table_name} add partition (dt='${DT}');"
done
