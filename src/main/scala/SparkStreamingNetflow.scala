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
case class Record(starttime: String)

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {
  @transient private var instance: SQLContext = null

  // Instantiate SQLContext on demand
  def getInstance(sparkContext: SparkContext): SQLContext = synchronized {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

object SparkStreamingNetflow extends Serializable {

  def sendToKafka(enrichKafkaLine: DStream[String]): Unit = {

    // ********** Start of write to Apache Kafka **********

    enrichKafkaLine.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val props = new Properties()
        //        props.put("metadata.broker.list", "bow-grd-res-01.bowdev.net:9092,bow-grd-res-02.bowdev.net:9092,bow-grd-res-03.bowdev.net:9092")
        props.put("metadata.broker.list", "vm-cluster-node2:9092,vm-cluster-node3:9092,vm-cluster-node4:9092")
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
            producer.send(new KeyedMessage[String, String]("netflow-output", msg))
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
      props.put("metadata.broker.list", "vm-cluster-node2:9092,vm-cluster-node3:9092,vm-cluster-node4:9092")
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
          producer.send(new KeyedMessage[String, String]("netflow-output", msg))
        }
      })
      producer.close()
    }

    // ********** End of write to Apache Kafka **********
    }

  def main(args: Array[String]) {

    if (args.length < 5) {
      System.err.println("Usage: SparkStreamingNetflow <zkQuorum> <group> <topics> <numThreads> <countryEnrichment>")
      System.exit(1)
    }

    val conf = ConfigFactory.load()
    val alertSQL = conf.getString("netflow-streaming.alertSql")
    val alertSQLList = conf.getStringList("netflow-streaming.alertSqlList")
    val argsCountryEnrichment = args(4)

    val format = new SimpleDateFormat("d/MM/y/hh/mm")
    val formatESIndexDate = new SimpleDateFormat("YYYY.MM.dd")
    // elasticsearch date format for automatic housekeeping  with python curator is YYYY.MM.DD e.g. 2015.03.23
    val hdfsPartitionDir = format.format(Calendar.getInstance().getTime())
    val ESIndexDate = formatESIndexDate.format(Calendar.getInstance().getTime())
    val elasticResource = "netflow-" + ESIndexDate + "/docs"
    val Array(zkQuorum, group, topics, numThreads, countryEnrichment) = args
    //    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("netflowkafka")
    // the jars array below is only needed when running on an IDE when the IDE points to a spark master
    // i.e. when the spark conf is something like this sparkConf.setMaster("spark://an-ip-address-or-hostname:7077")
    val jars = Array("C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-streaming-kafka_2.10\\1.3.0-cdh5.4.5\\spark-streaming-kafka_2.10-1.3.0-cdh5.4.5.jar",
      "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\kafka\\kafka_2.10\\0.8.0\\kafka_2.10-0.8.0.jar",
      "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-core_2.10\\1.3.0-cdh5.4.5\\spark-core_2.10-1.3.0-cdh5.4.5.jar",
      "C:\\Users\\801762473\\.m2\\repository\\com\\101tec\\zkclient\\0.3\\zkclient-0.3.jar",
      "C:\\Users\\801762473\\.m2\\repository\\com\\yammer\\metrics\\metrics-core\\2.2.0\\metrics-core-2.2.0.jar",
      "C:\\Users\\801762473\\.m2\\repository\\com\\esotericsoftware\\kryo\\kryo\\2.21\\kryo-2.21.jar",
      "C:\\Users\\801762473\\.m2\\repository\\org\\elasticsearch\\elasticsearch-spark_2.10\\2.1.0.Beta3\\elasticsearch-spark_2.10-2.1.0.Beta3.jar",
      "C:\\Users\\801762473\\.m2\\repository\\com\\maxmind\\db\\maxmind-db\\1.0.0\\maxmind-db-1.0.0.jar",
      "C:\\Users\\801762473\\.m2\\repository\\com\\maxmind\\geoip2\\geoip2\\2.1.0\\geoip2-2.1.0.jar",
      "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-hive_2.10\\1.3.0-cdh5.4.5\\spark-hive_2.10-1.3.0-cdh5.4.5.jar",
      "D:\\Bowen_Raw_Source\\IntelijProjects\\KafkaStreamingPOC\\target\\netflow-streaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar")

    // setup Spark
    val sparkConf = new SparkConf()
    sparkConf.setJars(jars)
    sparkConf.set("spark.serializer", classOf[KryoSerializer].getName) // Enable the Kryo serialization support with Spark for ES
    sparkConf.set("es.index.auto.create", "true") // set to auto create the ES index
    sparkConf.set("es.nodes", "192.168.160.72") // note, for multiple elastisearch nodes specify a csv list
//    sparkConf.setMaster("local[4]") // this specifies the master to be run in this IDe i.e. locally with 2 threads
    sparkConf.setMaster("spark://vm-cluster-node2:7077")
    sparkConf.set("spark.executor.memory", "512m")
    sparkConf.set("spark.driver.memory", "512m")
    sparkConf.set("spark.cores.max", "4")
    // Below line is the hostname or IP address for the driver to listen on. This is used for communicating with the executors and the standalone Master.
    sparkConf.set("spark.driver.host", "192.168.56.1")
//    sparkConf.setJars(jars)
    //    sparkConf.setMaster("spark://bow-grd-nn-02.bowdev.net:7077")
    //    sparkConf.setMaster("spark://quickstart.cloudera:7077")
    sparkConf.setAppName("netflowkafka")
    //    sparkConf.set("spark.executor.memory", "16g")
    //    sparkConf.set("spark.driver.memory", "4g")
    //    sparkConf.set("spark.driver.maxResultSize", "1g") // this is the default
    //    sparkConf.setJars(jars)
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //    ssc.checkpoint("hdfs://bow-grd-nn-01.bowdev.net/user/faganp/spark_checkpoint") // specify an hdfs directory if working on an hadoop platform
    ssc.checkpoint("spark_checkpoint") // specify an hdfs directory if working on an hadoop platform

    // start to process the lines
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2) // we may need to set the storage policy here
    //    val filteredLinesByLength = lines.filter(_.length > 1)
    //    val enrichLine = lines.map(line => line.split(",")(0).trim + line + "," + MaxMindSingleton.getInstance().getCountry(line.split(",")(3).trim))

    // set the enrichLine val based on if we want Contry Enrichment or not
    val enrichLine  = {
      if (countryEnrichment == true) {
        lines.map(line => line + "," + MaxMindSingleton.getInstance().getCountry(line.split(",")(3).trim))
      }
      else {
        lines.map(line => line)
      }
    }
//    enrichLine.print()

    /* Below only save rdd's with actual data in them and avoid the - java.lang.UnsupportedOperationException: empty collection
       exception being raised */
//    enrichLine.saveAsTextFiles("hdfs://bow-grd-nn-01.bowdev.net/user/faganp/spark-streaming/netflow_records", "/" + hdfsPartitionDir)
    enrichLine.foreachRDD( rdd => {
      if(!rdd.partitions.isEmpty)
        enrichLine.saveAsTextFiles("netflow_records", "/" + hdfsPartitionDir)
    })

    // ********** Start of rules based engine **********

    // convert csv RDD to space based RDD for below code
//    val spaceEnrichLine = enrichLine.map(x => x.split(","))

//    enrichLine.foreachRDD((rdd: RDD[String], time: Time) => {
//      // Get the singleton instance of SQLContext
//      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
//      import sqlContext.implicits._
//
//      // Convert RDD[String] to RDD[case class] to DataFrame
////      val wordsDataFrame = rdd.map(w => Record(w.split(",").toString)).toDF()
//      val wordsDataFrame = rdd.map(w => Record(w)).toDF()
//
//      // Register as table
//      wordsDataFrame.registerTempTable("words")
//
//      // Do word count on table using SQL and print it
//      val wordCountsDataFrame =
//        sqlContext.sql("select count(*) from words")
//      println(s"========= $time =========")
//      wordCountsDataFrame.show()
//    })

    // sc is an existing SparkContext.
//    val sqlContext = new org.apache.spark.sql.SQLContext(ssc)
    enrichLine.foreachRDD((rdd: RDD[String], time: Time) => {
      val sqlContext = SQLContextSingletonNetFlow.getInstance(rdd.sparkContext)
      // The schema is encoded in a string
      val schemaString = {
        if (countryEnrichment == true) {
          "starttime duration protocol srcaddr dir dstaddr dport state stos dtos totpkts totbytes country"
        }
        else {
          "starttime duration protocol srcaddr dir dstaddr dport state stos dtos totpkts totbytes"
        }
      }

      // Import Row.
      import org.apache.spark.sql.Row;

      // Import Spark SQL data types
      import org.apache.spark.sql.types.{StructType,StructField,StringType};

      // Generate the schema based on the string of schema
      val schema =
        StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

      // Convert records of the RDD (people) to Rows.
      // below the implicit conversion from datatypes can be done e.g. p(0).toInt if needed
      val rowRDD = rdd.map(_.split(",")).map(p =>
        Row(p(0), p(1).trim, p(2), p(3), p(4), p(5), p(6), p(7), p(8), p(9), p(10), p(11))
      )

      // Apply the schema to the RDD.
      val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

      // Register the DataFrames as a table.
      peopleDataFrame.registerTempTable("people")

      // SQL statements can be run by using the sql methods provided by sqlContext.
//      val results = sqlContext.sql("SELECT dir FROM people where dir = '->'")
      alertSQLList.toList.foreach(sqlToRun => {
        println("sqlToRun is : " + sqlToRun)
        val results = sqlContext.sql(sqlToRun)
        // convert the DataFrame to RDD[String]
        val alert = results.map(r => r.toString())
        alert.saveAsTextFile("alert")
        // iterate around the RDD[String] and send to kafka
        sendToKafka(alert)
      }
        // The results of SQL queries are DataFrames and support all the normal RDD operations.
        // The columns of a row in the result can be accessed by field index or by field name.
//        results.map(t => "Name: " + t(0)).collect().foreach(println)

      )
//      val results = sqlContext.sql(alertSQL)
//      // convert the DataFrame to RDD[String]
//      val alert = results.map(r => r.toString())
//      alert.saveAsTextFile("alert")
//      // iterate around the RDD[String] and send to kafka
//      sendToKafka(alert)
//
//      // The results of SQL queries are DataFrames and support all the normal RDD operations.
//      // The columns of a row in the result can be accessed by field index or by field name.
//      results.map(t => "Name: " + t(0)).collect().foreach(println)

    })

    // ********** End of rules based engine **********

    // ********** Start of write to Elasticsearch **********

    // prepare the elasticsearch DStream[String] with json formated data, p represents the whole enriched
    // netflow line, note: if you click or highlight a word in the code Ctrl-j in Intelij will tell you
    // the type of Scala object
//    val enrichLineES = enrichLine.map(p => "{\"StartTime\" : " + "\"" + p.split(",")(0) + "\"" + " , " +
//                                            "\"Dur\" : " + "\"" + p.split(",")(1) + "\"" + " , " +
//                                            "\"Proto\" : " + "\"" + p.split(",")(2) + "\"" + " , " +
//                                            "\"SrcAddr\" : " + "\"" + p.split(",")(3) + "\"" + " , " +
//                                            "\"Dir\" : " + "\"" + p.split(",")(5) + "\"" + " , " +
//                                            "\"DstAddr\" : " + "\"" + p.split(",")(6) + "\"" + " , " +
//                                            "\"Dport\" : " + "\"" + p.split(",")(7) + "\"" + " , " +
//                                            "\"State\" : " + "\"" + p.split(",")(8) + "\"" + " , " +
//                                            "\"sTos\" : " + "\"" + p.split(",")(9) + "\"" + " , " +
//                                            "\"sTos\" : " + "\"" + p.split(",")(10) + "\"" + " , " +
//                                            "\"dTos\" : " + "\"" + p.split(",")(11) + "\"" + " , " +
//                                            "\"TotPkts\" : " + "\"" + p.split(",")(12) + "\"" + " , " +
//                                            "\"TotBytes\" : " + "\"" + p.split(",")(13) + "\"" + " , " +
//                                            "\"Label\" : " + "\"" + p.split(",")(14) + "\"" + " , " +
//                                            "\"Country\" : " + "\"" + p.split(",")(3) + "\"}")
//
//    enrichLineES.foreachRDD { rdd =>
//      if(!rdd.partitions.isEmpty) {
//        val sparkConf = rdd.context
//        val sqlContext = new SQLContext(sparkConf)
//        val sendToEs = sqlContext.jsonRDD(rdd)
//        sendToEs.saveToEs(elasticResource)
//      }
//    }

    // ********** End of write to Elasticsearch **********

    // ********** Start of write to Apache Kafka **********

//    enrichLine.foreachRDD { rdd =>
//      rdd.foreachPartition { partitionOfRecords =>
//        val props = new Properties()
////        props.put("metadata.broker.list", "bow-grd-res-01.bowdev.net:9092,bow-grd-res-02.bowdev.net:9092,bow-grd-res-03.bowdev.net:9092")
//        props.put("metadata.broker.list", "quickstart.cloudera:9092")
//        props.put("serializer.class", "kafka.serializer.StringEncoder")
//
//        // some properties we might wish to set commented out below
//        //      props.put("compression.codec", codec.toString)
//        //      props.put("producer.type", "sync")
//        //      props.put("batch.num.messages", BatchSize.toString)
//        //      props.put("message.send.max.retries", maxRetries.toString)
//        //      props.put("request.required.acks", "-1")
//
//        val config = new ProducerConfig(props)
//        val producer = new Producer[String, String](config)
//        partitionOfRecords.foreach(row => {
//          val msg = row.toString
//          this.synchronized {
//            producer.send(new KeyedMessage[String, String]("flume.netflow_ss_output", msg))
//          }
//        })
//        producer.close()
//      }
//    }

//    sendToKafka(enrichLine)

    // ********** End of write to Apache Kafka **********

    ssc.start()
    ssc.awaitTermination()
    }

}
