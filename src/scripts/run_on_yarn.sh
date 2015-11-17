export HADOOP_USER_NAME=admin
spark-submit --class RandomNetflowGen --master yarn-client --driver-class-path '.:/opt/cloudera/parcels/CDH/lib/hive/lib/*' --driver-java-options \
'-Dspark.executor.extraClassPath=/opt/cloudera/parcels/CDH/lib/hive/lib/*' \
--driver-memory 512M  --executor-memory 512M --executor-cores 1 netflow-streaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar "hdfs://vm-cluster-node1:8020/user/admin/randomNetflow2" 4294 4 4 false
