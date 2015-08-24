# KafkaStreamingPOC

This project is a spark streaming project which consumes data from a kafka topic, output's data to a kafka topic and writes the data to hdfs.

Instructions for the Impatient

1. git clone https://github.com/faganpe/KafkaStreamingPOC.git
2. cd KafkaStreamingPOC
3. mvn package
4. run on a spark cluster with :
    spark-submit --jars $(echo *.jar | tr ' ' ',') --class RandomNetflowGen --master spark://c97f85a113e8:7077 --driver-class-path '/usr/lib/hive/lib/*' --driver-java-options '-Dspark.executor.extraClassPath=/usr/lib/hive/lib/*' sparkwordcount-0.0.1-SNAPSHOT.jar "hdfs://localhost:8020/user/faganpe/randomNetflow" 100000 2 2
5. see below for addtional debugging details, especially for setting up hive to work with spark

This project reads from a configuration file in the same directory as the maven packaged jar file called application.conf which overrides the inetrnal
packaged application.conf file in the 'resources' folder of the maven packed jar file.

It also contains a random netflow generator to genrate large volumes of netflow data into a hive parquet partitioned table.

A copy of hive-site.xml will need to be put in ${SPAK_HOME}/conf for spark to work correctly with the hive context in spark e.g.

cp /etc/hive/conf/hive-site.xml /etc/spark/conf.dist

There are two things which may need to be done, there are details below : -

1) Remove the .lck files in the hive metastore directory.
2) Depending on your unix user who you are running spark-submit as you meed need to grant permissions on the metastore DB if using the default non rdbms database.

You may need to remove the .lck files in the hive metastore directory e.g. : -

root@c97f85a113e8:/var/lib/hive/metastore/metastore_db# pwd
/var/lib/hive/metastore/metastore_db
root@c97f85a113e8:/var/lib/hive/metastore/metastore_db# ll
total 36
drwxr-xr-x 5 root root 4096 Mar 28 20:07 ./
drwxrwxrwt 3 hive hive 4096 Mar 28 20:07 ../
-rw-r--r-- 1 root root  608 Mar 28 20:07 README_DO_NOT_TOUCH_FILES.txt
-rw-r--r-- 1 root root   38 Mar 28 20:07 db.lck
-rw-r--r-- 1 root root    4 Mar 28 20:07 dbex.lck
drwxr-xr-x 2 root root 4096 Mar 28 20:07 log/
drwxr-xr-x 2 root root 4096 Mar 28 21:57 seg0/
-rw-r--r-- 1 root root  915 Mar 28 20:07 service.properties
drwxr-xr-x 2 root root 4096 Mar 28 20:07 tmp/
root@c97f85a113e8:/var/lib/hive/metastore/metastore_db#

root@c97f85a113e8:/var/lib/hive/metastore/metastore_db# rm *.lck
root@c97f85a113e8:/var/lib/hive/metastore/metastore_db# ll
total 28
drwxr-xr-x 5 root root 4096 Mar 30 20:15 ./
drwxrwxrwt 3 hive hive 4096 Mar 28 20:07 ../
-rw-r--r-- 1 root root  608 Mar 28 20:07 README_DO_NOT_TOUCH_FILES.txt
drwxr-xr-x 2 root root 4096 Mar 28 20:07 log/
drwxr-xr-x 2 root root 4096 Mar 28 21:57 seg0/
-rw-r--r-- 1 root root  915 Mar 28 20:07 service.properties
drwxr-xr-x 2 root root 4096 Mar 28 20:07 tmp/
root@c97f85a113e8:/var/lib/hive/metastore/metastore_db#

cd /var/lib/hive/metastore/metastore_db

chmod a+rwx . --recursive

root@c97f85a113e8:/var/lib/hive/metastore/metastore_db# ll
total 28
drwxrwxrwx 5 root root 4096 Mar 30 20:15 ./
drwxrwxrwt 3 hive hive 4096 Mar 28 20:07 ../
-rwxrwxrwx 1 root root  608 Mar 28 20:07 README_DO_NOT_TOUCH_FILES.txt*
drwxrwxrwx 2 root root 4096 Mar 28 20:07 log/
drwxrwxrwx 2 root root 4096 Mar 28 21:57 seg0/
-rwxrwxrwx 1 root root  915 Mar 28 20:07 service.properties*
drwxrwxrwx 2 root root 4096 Mar 28 20:07 tmp/
root@c97f85a113e8:/var/lib/hive/metastore/metastore_db#

If you receive this error : -

"Error while processing statement: FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. MetaException(message:Got exception: org.apache.hadoop.security.AccessControlException Permission denied: user=faganpe, access=WRITE, inode="/user/hive/warehouse":hive:supergroup:drwxrwxr-x at org.apache.hadoop.hdfs.server.namenode.DefaultAuthorizationProvider.checkFsPermission(DefaultAuthorizationProvider.java:257) at ....."

Then follow the guidance here http://gethue.com/hadoop-tutorial-hive-query-editor-with-hiveserver2-and/ , this may also help - http://www.cloudera.com/content/cloudera/en/documentation/cdh4/latest/CDH4-Installation-Guide/cdh4ig_topic_18_7.html

E.g.

hdfs@c97f85a113e8:~$ hdfs dfs -ls /user/hive
Found 1 items
drwxrwxr-x   - hive supergroup          0 2015-03-20 17:47 /user/hive/warehouse
hdfs@c97f85a113e8:~$ hdfs dfs -chmod 1777 /user/hive/warehouse
hdfs@c97f85a113e8:~$ hdfs dfs -ls /user/hive
Found 1 items
drwxrwxrwt   - hive supergroup          0 2015-03-20 17:47 /user/hive/warehouse
hdfs@c97f85a113e8:~$

As hdfs unix user : -

hdfs@c97f85a113e8:~$ hdfs dfs -chown hive:hive /user/hive/warehouse
hdfs@c97f85a113e8:~$ hdfs dfs -ls -h /user/hive/
Found 1 items
drwxrwxrwt   - hive hive          0 2015-03-20 17:47 /user/hive/warehouse
hdfs@c97f85a113e8:~$

If you see this error "UnsatisfiedLinkError: no snappyjava in java.library.path when running Spark MLLib Unit test within Intellij"
In Intelij you may need to add this to the Java VM run config : -

-Dorg.xerial.snappy.lib.name=libsnappyjava.jnilib -Dorg.xerial.snappy.tempdir=/tmp

See http://stackoverflow.com/questions/30039976/unsatisfiedlinkerror-no-snappyjava-in-java-library-path-when-running-spark-mlli for more details.

Enviroment Variables for Spark IDE : -

If you are using InteliJ or another IDE on windows you may need to set the HADOOP_HOME enviroment virable to something like - D:\hadoop-common-2.2.0-bin-master (https://github.com/srccodes/hadoop-common-2.2.0-bin).
Also you may need to set HADOOP_USER_NAME enviroment variable e.g. HADOOP_USER_NAME=admin so that the admin user is used when reading/writing to hdfs.

