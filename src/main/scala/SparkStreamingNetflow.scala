import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.hadoop.io.{MapWritable, NullWritable}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.serializer.KryoSerializer
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

/**
 * Created by faganpe on 23/02/15.
 */

object SparkStreamingNetflow extends Serializable {

  def main(args: Array[String]) {

    if (args.length < 4) {
      System.err.println("Usage: SparkStreamingNetflow <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val format = new SimpleDateFormat("d/MM/y/hh/mm")
    val formatESIndexDate = new SimpleDateFormat("YYYY.MM.dd")
    // elasticsearch date format for automatic housekeeping  with python curator is YYYY.MM.DD e.g. 2015.03.23
    val hdfsPartitionDir = format.format(Calendar.getInstance().getTime())
    val ESIndexDate = formatESIndexDate.format(Calendar.getInstance().getTime())
    val elasticResource = "netflow-" + ESIndexDate + "/docs"
    val Array(zkQuorum, group, topics, numThreads) = args
//    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("netflowkafka")
    // the jars array below is only needed when running on an IDE when the IDE points to a spark master
    // i.e. when the spark conf is something like this sparkConf.setMaster("spark://an-ip-address-or-hostname:7077")
//    val jars = Array("/home/faganp/.m2/repository/org/apache/spark/spark-streaming-kafka_2.10/1.2.0-cdh5.3.0/spark-streaming-kafka_2.10-1.2.0-cdh5.3.0.jar",
//                    "/home/faganp/.m2/repository/org/apache/kafka/kafka_2.10/0.8.0/kafka_2.10-0.8.0.jar",
//                    "/home/faganp/.m2/repository/org/apache/spark/spark-core_2.10/1.2.0-cdh5.3.0/spark-core_2.10-1.2.0-cdh5.3.0.jar",
//                    "/home/faganp/.m2/repository/com/101tec/zkclient/0.3/zkclient-0.3.jar",
//                    "/home/faganp/.m2/repository/com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar",
//                    "/home/faganp/.m2/repository/com/esotericsoftware/kryo/kryo/2.21/kryo-2.21.jar",
//                    "/home/faganp/.m2/repository/org/elasticsearch/elasticsearch-spark_2.10/2.1.0.Beta3/elasticsearch-spark_2.10-2.1.0.Beta3.jar",
//                    "/home/faganp/.m2/repository/com/maxmind/db/maxmind-db/1.0.0/maxmind-db-1.0.0.jar",
//                    "/home/faganp/.m2/repository/com/maxmind/geoip2/geoip2/2.1.0/geoip2-2.1.0.jar",
//                    "/home/faganp/IntelijProjects/KafkaStreamingPOC/target/sparkwordcount-0.0.1-SNAPSHOT.jar")

    // setup Spark
    val sparkConf = new SparkConf()
//    sparkConf.set("spark.serializer", classOf[KryoSerializer].getName) // Enable the Kryo serialization support with Spark for ES
    sparkConf.set("es.index.auto.create", "true") // set to auto create the ES index
    sparkConf.set("es.nodes", "192.168.160.72") // note, for multiple elastisearch nodes specify a csv list
//    sparkConf.setMaster("local[4]") // this specifies the master to be run in this IDe i.e. locally with 2 threads
//    sparkConf.setMaster("spark://bow-grd-nn-02.bowdev.net:7077")
//    sparkConf.setMaster("spark://quickstart.cloudera:7077")
    sparkConf.setAppName("netflowkafka")
    sparkConf.set("spark.executor.memory", "16g")
    sparkConf.set("spark.driver.memory", "4g")
//    sparkConf.set("spark.driver.maxResultSize", "1g") // this is the default
//    sparkConf.setJars(jars)
    val ssc =  new StreamingContext(sparkConf, Seconds(30))

    ssc.checkpoint("hdfs://bow-grd-nn-01.bowdev.net/user/faganp/spark_checkpoint") // specify an hdfs directory if working on an hadoop platform

    // start to process the lines
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2) // we may need to set the storage policy here
//    val filteredLinesByLength = lines.filter(_.length > 1)
//    val enrichLine = lines.map(line => line.split(",")(0).trim + line + "," + MaxMindSingleton.getInstance().getCountry(line.split(",")(3).trim))
    val enrichLine = lines.map(line => line + "," + MaxMindSingleton.getInstance().getCountry(line.split(",")(3).trim))
//    enrichLine.print()

    enrichLine.saveAsTextFiles("hdfs://bow-grd-nn-01.bowdev.net/user/faganp/spark-streaming/netflow_records", "/" + hdfsPartitionDir)

    // ********** Start of write to Elasticsearch **********

    // prepare the elasticsearch DStream[String] with json formated data, p represents the whole enriched
    // netflow line, note: if you click or highlight a word in the code Ctrl-j in Intelij will tell you
    // the type of Scala object
    val enrichLineES = enrichLine.map(p => "{\"StartTime\" : " + "\"" + p.split(",")(0) + "\"" + " , " +
                                            "\"Dur\" : " + "\"" + p.split(",")(1) + "\"" + " , " +
                                            "\"Proto\" : " + "\"" + p.split(",")(2) + "\"" + " , " +
                                            "\"SrcAddr\" : " + "\"" + p.split(",")(3) + "\"" + " , " +
                                            "\"Dir\" : " + "\"" + p.split(",")(5) + "\"" + " , " +
                                            "\"DstAddr\" : " + "\"" + p.split(",")(6) + "\"" + " , " +
                                            "\"Dport\" : " + "\"" + p.split(",")(7) + "\"" + " , " +
                                            "\"State\" : " + "\"" + p.split(",")(8) + "\"" + " , " +
                                            "\"sTos\" : " + "\"" + p.split(",")(9) + "\"" + " , " +
                                            "\"sTos\" : " + "\"" + p.split(",")(10) + "\"" + " , " +
                                            "\"dTos\" : " + "\"" + p.split(",")(11) + "\"" + " , " +
                                            "\"TotPkts\" : " + "\"" + p.split(",")(12) + "\"" + " , " +
                                            "\"TotBytes\" : " + "\"" + p.split(",")(13) + "\"" + " , " +
                                            "\"Label\" : " + "\"" + p.split(",")(14) + "\"" + " , " +
                                            "\"Country\" : " + "\"" + p.split(",")(3) + "\"}")

//    val enrichLineES = enrichLine.map(p => "{\"StartTime\" : " + "\"" + "00:00:00" + "\"" + " , " +
//      "\"Dur\" : " + "\"" + "0.000001" + "\"" + " , " +
//      "\"Country\" : " + "\"" + "UK" + "\"}")

    // print 10 lines of the json for visual / debugging purposes
    enrichLineES.print()

    enrichLineES.foreachRDD { rdd =>
      val sparkConf = rdd.context
      val sqlContext = new SQLContext(sparkConf)
      val sendToEs = sqlContext.jsonRDD(rdd)
      sendToEs.saveToEs(elasticResource)
    }

    // ********** End of write to Elasticsearch **********

    // ********** Start of write to Apache Kafka **********

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

    // ********** End of write to Apache Kafka **********

    ssc.start()
    ssc.awaitTermination()
    }

}
