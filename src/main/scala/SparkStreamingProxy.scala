import java.io
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import Utils.MongoRules
import kafka.serializer.StringDecoder

import scala.collection.mutable.ArrayBuffer

//import SQLContextSingleton
import com.typesafe.config.ConfigFactory
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

/**
 * Created by 801762473 on 27/10/2015.
 */
object SparkStreamingProxy extends Serializable {

  def sendToKafka(enrichKafkaLine: DStream[String]): Unit = {

    // ********** Start of write to Apache Kafka **********

    println("In the sendToKafka DStream method")

    enrichKafkaLine.foreachRDD { rdd =>
      rdd.foreachPartition { partitionOfRecords =>
        val props = new Properties()
        //        props.put("metadata.broker.list", "bow-grd-res-01.bowdev.net:9092,bow-grd-res-02.bowdev.net:9092,bow-grd-res-03.bowdev.net:9092")
//        props.put("metadata.broker.list", "vm-cluster-node2:9092,vm-cluster-node3:9092,vm-cluster-node4:9092")
        props.put("metadata.broker.list", "localhost:9092")
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
          println("DStream : " + msg)
          this.synchronized {
            producer.send(new KeyedMessage[String, String]("proxy-output", msg))
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
//      props.put("metadata.broker.list", "vm-cluster-node2:9092,vm-cluster-node3:9092,vm-cluster-node4:9092")
      props.put("metadata.broker.list", "localhost:9092")
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      props.put("producer.type", "async")
      // some properties we might wish to set commented out below
      //      props.put("compression.codec", codec.toString)
      //      props.put("producer.type", "sync")
      //      props.put("batch.num.messages", BatchSize.toString)
      //      props.put("message.send.max.retries", maxRetries.toString)
      //      props.put("request.required.acks", "-1")

      val config = new ProducerConfig(props)
      val producer = new Producer[String, String](config)
      partitionOfRecords.foreach(row => {
        //        val msg = row.toString
//        println("About to send message")
//        val msg = "Hello Paul"
        val msg = row.toString
//        println("RDD proxy-output : " + msg)
        this.synchronized {
          producer.send(new KeyedMessage[String, String]("proxy-output", msg))
        }
      })
      producer.close()
    }

    def sendToKafka(enrichLine: DStream[_ >: String with (String, String) <: io.Serializable]): Unit = {

    }

    // ********** End of write to Apache Kafka **********
  }


  /* Start of parsing space delimted line to handle double quoted elements as one string in the array */
  // note, this is a bit hacky maybe a proper csv parser should be used

  def lineParse(pArrayWholeLine: Array[String]): Array[String] = {

    var replaceArrayWholeLine = ArrayBuffer[String]()
    var replaceStr: String = ""
    var startFlag: Boolean = false

    for(myString <- pArrayWholeLine) {
      if (myString.matches("^\".*\"$")) {
        replaceArrayWholeLine += myString + " "
      }
      else if (myString.matches("^\".*")) {
        replaceStr += myString + " "
        startFlag = true
      }
      else if (myString.matches(".*\"$")) {
        replaceStr += myString + " "
        replaceArrayWholeLine += replaceStr
        replaceStr = "" // reset the replace string
        startFlag = false
      }
      else {
        if (startFlag) {
          replaceStr += myString + " "
        }
        else {
          replaceArrayWholeLine += myString + " "
        }
      }
    }

    return replaceArrayWholeLine.toArray
  }

  /* End of parsing space delimted line to handle double quoted elements as one string in the array */

  def main(args: Array[String]) {

    if (args.length < 5) {
      System.err.println("Usage: SparkStreamingNetflow <zkQuorum> <group> <topics> <numThreads> <countryEnrichment>")
      System.exit(1)
    }

    val conf = ConfigFactory.load()
    val alertSQL = conf.getString("proxylog-streaming.alertSql")
    val alertSQLList = conf.getStringList("proxylog-streaming.alertSqlList")
    val alertSQLType = conf.getString("proxylog-streaming.alertSQLType")
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
//    val jars = Array("C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-streaming-kafka_2.10\\1.3.0-cdh5.4.5\\spark-streaming-kafka_2.10-1.3.0-cdh5.4.5.jar",
//      //      "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\kafka\\kafka_2.10\\0.8.0\\kafka_2.10-0.8.0.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\kafka\\kafka_2.10\\0.8.2.0\\kafka_2.10-0.8.2.0.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-core_2.10\\1.3.0-cdh5.4.5\\spark-core_2.10-1.3.0-cdh5.4.5.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\com\\101tec\\zkclient\\0.3\\zkclient-0.3.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\com\\yammer\\metrics\\metrics-core\\2.2.0\\metrics-core-2.2.0.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\com\\esotericsoftware\\kryo\\kryo\\2.21\\kryo-2.21.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\org\\elasticsearch\\elasticsearch-spark_2.10\\2.1.0.Beta3\\elasticsearch-spark_2.10-2.1.0.Beta3.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\com\\maxmind\\db\\maxmind-db\\1.0.0\\maxmind-db-1.0.0.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\com\\maxmind\\geoip2\\geoip2\\2.1.0\\geoip2-2.1.0.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-hive_2.10\\1.3.0-cdh5.4.5\\spark-hive_2.10-1.3.0-cdh5.4.5.jar",
//      "D:\\Bowen_Raw_Source\\IntelijProjects\\KafkaStreamingPOC\\target\\netflow-streaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar")

    // setup Spark
    val sparkConf = new SparkConf()
//    sparkConf.setJars(jars)
    sparkConf.set("spark.serializer", classOf[KryoSerializer].getName) // Enable the Kryo serialization support with Spark for ES
    sparkConf.set("es.index.auto.create", "true") // set to auto create the ES index
    sparkConf.set("es.nodes", "192.168.160.72") // note, for multiple elastisearch nodes specify a csv list
    sparkConf.setMaster("local[4]") // this specifies the master to be run in this IDe i.e. locally with 2 threads
//    sparkConf.setMaster("spark://vm-cluster-node2:7077")
    sparkConf.set("spark.executor.memory", "512m")
    sparkConf.set("spark.driver.memory", "512m")
//    sparkConf.set("spark.cores.max", "4")
    // Below line is the hostname or IP address for the driver to listen on. This is used for communicating with the executors and the standalone Master.
//    sparkConf.set("spark.driver.host", "192.168.56.1")
    //    sparkConf.setJars(jars)
    //    sparkConf.setMaster("spark://bow-grd-nn-02.bowdev.net:7077")
    //    sparkConf.setMaster("spark://quickstart.cloudera:7077")
    sparkConf.setAppName("proxylogskafka")
    //    sparkConf.set("spark.executor.memory", "16g")
    //    sparkConf.set("spark.driver.memory", "4g")
    //    sparkConf.set("spark.driver.maxResultSize", "1g") // this is the default
    //    sparkConf.setJars(jars)
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    //    ssc.checkpoint("hdfs://bow-grd-nn-01.bowdev.net/user/faganp/spark_checkpoint") // specify an hdfs directory if working on an hadoop platform
    ssc.checkpoint("spark_checkpoint") // specify an hdfs directory if working on an hadoop platform

    // start to process the lines
//    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
//    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2) // we may need to set the storage policy here

//    val topicsSet = topics.split(",").toSet
    val topicsSet = Set("netflow-input")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "localhost:9092")
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val enrichLine  = {
      if (countryEnrichment == true) {
        lines.map(line => line + "," + MaxMindSingleton.getInstance().getCountry(line._2.split(",")(3).trim))
//        lines.map(line => line)
      }
      else {
        lines.map(line => line._2)
      }
    }

    /* Below only save rdd's with actual data in them and avoid the - java.lang.UnsupportedOperationException: empty collection
       exception being raised */
    //    enrichLine.saveAsTextFiles("hdfs://bow-grd-nn-01.bowdev.net/user/faganp/spark-streaming/netflow_records", "/" + hdfsPartitionDir)
    //    enrichLine.foreachRDD( rdd => {
    //      if(!rdd.partitions.isEmpty)
    //        enrichLine.saveAsTextFiles("netflow_records", "/" + hdfsPartitionDir)
    //    })

    // ********** Start of rules based engine **********

    enrichLine.foreachRDD((rdd: RDD[String], time: Time) => {
      val sqlContext = SQLContextSingletonNetFlow.getInstance(rdd.sparkContext)
      // The schema is encoded in a string, tak eoff the dashes
      val schemaString = {
        if (countryEnrichment == true) {
          "starttime duration protocol srcaddr dir dstaddr dport state stos dtos totpkts totbytes country"
        }
        else {
          "date time time-taken c-ip sc-status s-action sc-bytes cs-bytes cs-method cs-uri-scheme cs-host cs-uri-port cs-uri-path cs-uri-query cs-username cs-auth-group s-supplier-name rs(Content-Type) cs(Referer) cs(User-Agent) sc-filter-result cscategories x-virus-id s-ip s-action x-exception-id r-ip"
        }
      }

      // Import Row.
      import org.apache.spark.sql.Row;

      // Import Spark SQL data types
      import org.apache.spark.sql.types.{StringType, StructField, StructType};

      // Generate the schema based on the string of schema
      val schema =
        StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

      // Convert records of the RDD (people) to Rows.
      // below the implicit conversion from datatypes can be done e.g. p(0).toInt if needed
      // note, case class method of doing this can only handle 22 elemnts?
      val rowRDD = rdd.map(_.split(" ")).map(p => {
        val p1 = lineParse(p)
        Row(p1(0), p1(1).trim, p1(2), p1(3), p1(4), p1(5), p1(6), p1(7), p1(8), p1(9), p1(10), p1(11), p1(12), p1(13), p1(14), p1(15), p1(16), p1(17), p1(18), p1(19), p1(20), p1(21), p1(22), p1(23), p1(24), p1(25), p1(26))
        }
      )
//
//      // Apply the schema to the RDD.
      val alertsDataFrame = sqlContext.createDataFrame(rowRDD, schema)

      // Register the DataFrames as a table.
      alertsDataFrame.registerTempTable("alerts")

      // SQL statements can be run by using the sql methods provided by sqlContext.
//      val results = sqlContext.sql("SELECT dir FROM people where dir = '->'")

      var alertSQL: Array[AnyRef] = null
      if (alertSQLType == "alertSqlList") {
        println("Using alertSqlList")
        alertSQL = alertSQLList.toArray()
      }
      else if (alertSQLType == "alertSqlMongoDB") {
        println("Using mongo DB")
        alertSQL = MongoRules.retreiveSQLRules()
      }

//      val alertSQL = alertSQLList.toArray

      alertSQL.foreach( sqlToRun => {
        println("Running SQL Alert: " + sqlToRun)
        val results = sqlContext.sql(sqlToRun.toString)
        val alert = results.map(r => r.toString())
        println("Number of results in alert is : " + alert.count())
        sendToKafka(alert)
      })

    })
    // End of Comment Out

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
    //        props.put("metadata.broker.list", "vm-cluster-node2:9092,vm-cluster-node3:9092,vm-cluster-node4:9092")
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
    //            producer.send(new KeyedMessage[String, String]("netflow-output2", msg))
    //          }
    //        })
    //        producer.close()
    //      }
    //    }

    // ********** End of write to Apache Kafka **********

    ssc.start()
    ssc.awaitTermination()
  }

}