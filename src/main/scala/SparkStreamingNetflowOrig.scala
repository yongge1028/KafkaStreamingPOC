import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.Calendar
import java.text.SimpleDateFormat

/**
 * Created by faganpe on 23/02/15.
 */
object SparkStreamingNetflowOrig extends Serializable {

  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: SparkStreamingNetflow <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val format = new SimpleDateFormat("y-MM-d")
    val hdfsPartitionDir = "dt=" + format.format(Calendar.getInstance().getTime())
    val Array(zkQuorum, group, topics, numThreads) = args
//    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("netflowkafka")
    val jars = Array("/home/faganp/.ivy2/cache/org.apache.spark/spark-streaming-kafka_2.10/jars/spark-streaming-kafka_2.10-1.2.0-cdh5.3.0.jar",
                    "/home/faganp/.ivy2/cache/org.apache.kafka/kafka_2.10/jars/kafka_2.10-0.8.0.jar",
                    "/home/faganp/.ivy2/cache/com.101tec/zkclient/jars/zkclient-0.3.jar",
                    "/home/faganp/.ivy2/cache/com.yammer.metrics/metrics-core/jars/metrics-core-2.2.0.jar",
//                    "/home/faganp/IntelijProjects/KafkaStreamingPOC/target/scala-2.10/kafkastreamingpoc_2.10-1.0.jar")
                    "/home/faganp/IntelijProjects/KafkaStreamingPOC/target/sparkwordcount-0.0.1-SNAPSHOT.jar")

    // setup Spark
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[4]")
//    sparkConf.setMaster("spark://bow-grd-nn-02.bowdev.net:7077")
    sparkConf.setAppName("netflowkafka")
    sparkConf.set("spark.executor.memory", "8g")
    sparkConf.set("spark.driver.memory", "8g")
    sparkConf.set("spark.driver.maxResultSize", "8g") // this is the default
    sparkConf.setJars(jars)
    val ssc =  new StreamingContext(sparkConf, Seconds(10))
    ssc.checkpoint("hdfs://bow-grd-nn-01.bowdev.net:8020/user/faganp/spark_checkpoint")

    // start to process the lines
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val filteredLinesByLength = lines.filter(_.length > 1)
//    val enrichLine = filteredLinesByLength.map(line => line + "," + MaxMindSingleton.getInstance().getCountry(line.split(",")(1)))
//    val enrichLine = filteredLinesByLength.map(line => line + "," + line(1))
    val enrichLine = filteredLinesByLength.map(line => line + "," + MaxMindSingleton.getInstance().getCountry((line.split(",")(1).trim)))
    enrichLine.print()
//    val enrichLine = filteredLinesByLength.map(line => line.split(",")(1))
//    enrichLine.saveAsTextFiles("hdfs://bow-grd-nn-01.bowdev.net/user/faganp/spark-streaming/netflow_records", "/" + hdfsPartitionDir)
    enrichLine.saveAsTextFiles("hdfs://bow-grd-nn-01.bowdev.net/user/faganp/spark-streaming/netflow-records/2015-03-01")

    // Now send to Kafka
    enrichLine.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val props = new Properties()
        props.put("metadata.broker.list", "bow-grd-res-01.bowdev.net:9092,bow-grd-res-02.bowdev.net:9092,bow-grd-res-03.bowdev.net:9092")
        props.put("serializer.class", "kafka.serializer.StringEncoder")

        // some properties we might wish to set commented out below
        //      props.put("compression.codec", codec.toString)
        //      props.put("producer.type", "sync")
        //      props.put("batch.num.messages", BatchSize.toString)
        //      props.put("message.send.max.retries", maxRetries.toString)
        //      props.put("request.required.acks", "-1")

        val config = new ProducerConfig(props)
        val producer = new Producer[String, String](config)
        partitionOfRecords.foreach(row => {
          val msg = row.toString
          this.synchronized {
            producer.send(new KeyedMessage[String, String]("flume.netflow_ss_output", msg))
          }
        })
        producer.close()
      }
    }

    ssc.start()
    ssc.awaitTermination()
    }

}
