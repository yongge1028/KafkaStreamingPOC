import java.util.Properties

import com.typesafe.config.ConfigFactory
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.hadoop.io.{MapWritable, NullWritable}
import org.apache.spark.examples.sql.hive.HiveFromSpark.Record
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.rdd.EsSpark

//import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Time, Seconds, StreamingContext}
import java.util.Calendar
import java.text.SimpleDateFormat
import org.elasticsearch.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.elasticsearch.spark.sql._
import scala.collection.JavaConversions._

/**
 * Created by faganpe on 23/02/15.
 */

/** Case class for converting RDD to DataFrame */
// Define the schema using a case class.
// Note: Case classes in Scala 2.10 can support only up to 22 fields. To work around this limit,
// you can use custom classes that implement the Product interface.
case class NetflowRecord(starttime: String)

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingletonNetFlow {
  @transient private var instance: SQLContext = null

  // Instantiate SQLContext on demand
  def getInstance(sparkContext: SparkContext): SQLContext = synchronized {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

object SparkStreamingNetflowJson extends Serializable {

  def sendToKafka(enrichKafkaLine: DStream[String]): Unit = {

    // ********** Start of write to Apache Kafka **********

    enrichKafkaLine.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val props = new Properties()
        //        props.put("metadata.broker.list", "bow-grd-res-01.bowdev.net:9092,bow-grd-res-02.bowdev.net:9092,bow-grd-res-03.bowdev.net:9092")
        props.put("metadata.broker.list", "quickstart.cloudera:9092")
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

    // ********** End of write to Apache Kafka **********

  }

  def sendToKafka(enrichKafkaLine: RDD[String]): Unit = {

    println("In the sendToKafka RDD method")

    // ********** Start of write to Apache Kafka **********

    enrichKafkaLine.foreachPartition { partitionOfRecords =>
      println("In the sendToKafka RDD partitionOfRecords")
      val props = new Properties()
      //        props.put("metadata.broker.list", "bow-grd-res-01.bowdev.net:9092,bow-grd-res-02.bowdev.net:9092,bow-grd-res-03.bowdev.net:9092")
      props.put("metadata.broker.list", "quickstart.cloudera:9092")
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

    // ********** End of write to Apache Kafka **********
    }

  def parser(json: String): String = {
    return json
  }

    def main(args: Array[String]) {

      if (args.length < 4) {
        System.err.println("Usage: SparkStreamingNetflow <zkQuorum> <group> <topics> <numThreads>")
        System.exit(1)
      }

      val conf = ConfigFactory.load()
      val alertSQL = conf.getString("netflow-streaming.alertSql")
      val alertSQLList = conf.getStringList("netflow-streaming.alertSqlList")
      val hdfsURI = conf.getString("netflow-streaming.hdfsURI")
      val sscDuration = conf.getLong("netflow-streaming.sscDuration")

      // elasticsearch date format for automatic housekeeping  with python curator is YYYY.MM.DD e.g. 2015.03.23
      val Array(zkQuorum, group, topics, numThreads) = args
      //    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("netflowkafka")
      // the jars array below is only needed when running on an IDE when the IDE points to a spark master
      // i.e. when the spark conf is something like this sparkConf.setMaster("spark://an-ip-address-or-hostname:7077")
//      val jars = Array("/Users/faganpe/.m2/repository/org/apache/spark/spark-streaming-kafka_2.10/1.3.0-cdh5.4.1/spark-streaming-kafka_2.10-1.3.0-cdh5.4.1.jar",
//        "/Users/faganpe/.m2/repository/org/apache/kafka/kafka_2.10/0.8.0/kafka_2.10-0.8.0.jar",
//        "/Users/faganpe/.m2/repository/org/apache/spark/spark-core_2.10/1.3.0-cdh5.4.1/spark-core_2.10-1.3.0-cdh5.4.1.jar",
//        "/Users/faganpe/.m2/repository/com/101tec/zkclient/0.3/zkclient-0.3.jar",
//        "/Users/faganpe/.m2/repository/com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar",
//        "/Users/faganpe/.m2/repository/com/esotericsoftware/kryo/kryo/2.21/kryo-2.21.jar",
//        "/Users/faganpe/.m2/repository/org/elasticsearch/elasticsearch-spark_2.10/2.1.0.Beta3/elasticsearch-spark_2.10-2.1.0.Beta3.jar",
//        "/Users/faganpe/.m2/repository/com/maxmind/db/maxmind-db/1.0.0/maxmind-db-1.0.0.jar",
//        "/Users/faganpe/.m2/repository/com/maxmind/geoip2/geoip2/2.1.0/geoip2-2.1.0.jar",
//        "/Users/faganpe/IntelijProjects/KafkaStreamingPOC/target/sparkwordcount-0.0.1-SNAPSHOT.jar")

      // setup Spark
      val sparkConf = new SparkConf()
//      sparkConf.setJars(jars)
      sparkConf.set("spark.serializer", classOf[KryoSerializer].getName) // Enable the Kryo serialization support with Spark for ES
      sparkConf.setMaster("local[4]") // this specifies the master to be run in this IDe i.e. locally with 2 threads
      //    sparkConf.setMaster("spark://bow-grd-nn-02.bowdev.net:7077")
//      sparkConf.setMaster("spark://quickstart.cloudera:7077")
      sparkConf.setAppName("netflowkafka")
      //    sparkConf.set("spark.executor.memory", "16g")
      //    sparkConf.set("spark.driver.memory", "4g")
      //    sparkConf.set("spark.driver.maxResultSize", "1g") // this is the default
      //    sparkConf.setJars(jars)
      val ssc = new StreamingContext(sparkConf, Seconds(sscDuration))

      //    ssc.checkpoint("hdfs://bow-grd-nn-01.bowdev.net/user/faganp/spark_checkpoint") // specify an hdfs directory if working on an hadoop platform
      ssc.checkpoint("spark_checkpoint") // specify an hdfs directory if working on an hadoop platform

      // start to process the lines
      val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
      //    val json_stream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2) // we may need to set the storage policy here
      val json_stream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap) // we may need to set the storage policy here
      var netflowDstream = json_stream.map(_._2).map(parser)
      netflowDstream.print()
      netflowDstream.saveAsTextFiles(hdfsURI + "/json", "json")
//      sendToKafka(netflowDstream)

          netflowDstream.foreachRDD((rdd: RDD[String], time: Time) => {
      //      println("In the foreachRDD loop")
            val sqlContext = SQLContextSingletonNetFlow.getInstance(rdd.sparkContext)
      //      // Convert to Json and send to
            if (!rdd.partitions.isEmpty) {
              println("Json RDD not empty")
              val jsonRdd = sqlContext.jsonRDD(rdd) // .registerTempTable("testjson")
//              sqlContext.sql("SELECT * FROM testjson")
//              jsonRdd.saveAsParquetFile("df/parquet", )
              jsonRdd.save("parquet", SaveMode.Append)
//              jsonRdd.saveAsTable("netflow", SaveMode.Append)
            }
      //
            })

      ssc.start()
      ssc.awaitTermination()
    }

}
