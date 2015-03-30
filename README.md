# KafkaStreamingPOC

This project is a spark streaming project which consumes data from a kafka topic, output's data to a kafka topic and writes the data to hdfs.

It also contains a random netflow generator to genrate large volumes of netflow data into hdfs.

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