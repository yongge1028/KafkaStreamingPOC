spark-submit --jars $(echo *.jar | tr ' ' ',') --class SparkStreamingNetflow --master spark://bow-grd-nn-02.bowdev.net:7077 sparkwordcount-0.0.1-SNAPSHOT.jar "bow-grd-res-01.bowdev.net:2181,bow-grd-res-02.bowdev.net:2181,bow-grd-res-03.b
owdev.net:2181" 1 "flume.netflow_test3" 2
spark-submit --jars $(echo *.jar | tr ' ' ',') --class RandomNetflowGen --master spark://79d4dd97b170:7077 sparkwordcount-0.0.1-SNAPSHOT.jar "hdfs://localhost:8020/user/faganpe/randomNetflow" 100000 2 2 true
spark-submit --jars $(echo *.jar | tr ' ' ',') --class RandomNetflowGen --master spark://c97f85a113e8:7077 --driver-class-path '/usr/lib/hive/lib/*' --driver-java-options '-Dspark.executor.extraClassPath=/usr/lib/hive/lib/*' sparkwordcount-0.0.1-SNAPSHOT.jar "hdfs://localhost:8020/user/faganpe/randomNetflow" 100000 2 2 tru
spark-submit --jars $(echo *.jar | tr ' ' ',') --class RandomNetflowGen --master spark://0f37edb37c68:7077 --driver-class-path '/usr/lib/hive/lib/*' --driver-java-options '-Dspark.executor.extraClassPath=/usr/lib/hive/lib/*' netflow-streaming-0.0.1-SNAPSHOT.jar 100000 2 2 false