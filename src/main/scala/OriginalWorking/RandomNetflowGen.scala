package OriginalWorking

/**
 * Created by faganp on 3/19/15.
 */

import Utils.PopulateRandomString
import com.typesafe.config.ConfigFactory

//import org.apache.spark.sql.catalyst.types.{StringType, StructField, StructType} // spark 1.2 codeline

//import java.util.Properties

//import _root_.kafka.producer.Producer
//import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.serializer.KryoSerializer
//import org.elasticsearch.spark.rdd.EsSpark

//import org.apache.spark.sql.SQLContext
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.elasticsearch.spark._
//import org.apache.spark.SparkContext._
//import org.apache.spark.sql._
//import org.elasticsearch.spark.sql._
import com.google.common.net.InetAddresses

import scala.Array._
//import util.Properties
import java.text.SimpleDateFormat;

/**
 * Created by faganpe on 17/03/15.
 */

object RandomNetflowGen extends Serializable {

  def stripChars(s:String, ch:String)= s filterNot (ch contains _)

  def getIPGenRand(randNum: Int): String = {
    //    val r = scala.util.Random
    if (randNum % 2 == 0) getIPRand()
    else getIPAddressSkew("132.146.5")
    //      getIPAddressSkew("132.146.5")
  }
  //
  //  /* End of the random generation values used to influence the data that is produced */
  //
  def getIPAddressSkew(IPSubnet: String): String = {
    val r = scala.util.Random
    val dotCount = IPSubnet.count(_ == '.') // let's count the number of dots
    if (dotCount == 3) IPSubnet // return the complete IP address without making anything up
    else if (dotCount == 2) IPSubnet + "." + r.nextInt(255)
    else if (dotCount == 1) IPSubnet + "." + r.nextInt(255) + "." + r.nextInt(255)
    else IPSubnet // otherwise just return the original ip string
  }

  def getIPRand(): String = {
    val r = scala.util.Random
    InetAddresses.fromInteger(r.nextInt()).getHostAddress()
  }

  // randNum method limit's random number of integers i.e. if 100 passed in number returned can be in the range 0 to 99
  def randNum(ranNum: Int): Int = {
    val r = scala.util.Random
    r.nextInt(ranNum)
  }

  /* Start of the random generation values used to influence the data that is produced */

  val r = scala.util.Random

  def main(args: Array[String]) {

    val conf = ConfigFactory.load()
    val appName = conf.getString("netflow-app.name")
//    val appRandomDistributionMin = conf.getInt("netflow-app.randomDistributionMin")
//    val appRandomDistributionMax = conf.getInt("netflow-app.randomDistributionMax")
    println("The application name  is: " + appName)

    if (args.length != 5) {
      System.err.println("Usage: " + "hdfs://quickstart.cloudera:8020/user/cloudera/randomNetflow <numRecords> <numFilesPerDir> <numDirectories> <CountryEnrichment>")
      System.err.println("Example: " + "hdfs://quickstart.cloudera:8020/user/cloudera/randomNetflow 30000000 4 10 true")
      System.exit(1)
    }
    else {
      println("Supplied arguments to the program are : " + args(0).toString + " " + args(1).toInt + " " + args(2).toInt + " " + args(3) + " " + args(4))
    }

    val format = new SimpleDateFormat("dd-MM-yyyy-hh-mm-ss")
    //    val hdfsPartitionDir = format.format(Calendar.getInstance().getTime())

    // setup Spark
    val sparkConf = new SparkConf()
//    sparkConf.setMaster("local[4]")
//    sparkConf.setMaster("spark://vm-cluster-node2:7077")
////    sparkConf.setMaster("yarn-cluster")
    sparkConf.setMaster("spark://quickstart.cloudera:7077")
//    //    sparkConf.setMaster("spark://79d4dd97b170:7077")
//        sparkConf.set("spark.executor.memory", "256m")
//        sparkConf.set("spark.driver.memory", "256m")
//        sparkConf.set("spark.cores.max", "4")
//    sparkConf.set("spark.worker.cleanup.enabled", "true")
//    sparkConf.set("spark.worker.cleanup.interval", "1")
//    sparkConf.set("spark.worker.cleanup.appDataTtl", "30")

    /* Change to Kyro Serialization */
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("es.index.auto.create", "true") // set to auto create the ES index
//    sparkConf.set("es.nodes", "192.168.99.100") // note, for multiple elastisearch nodes specify a csv list
    sparkConf.set("es.nodes", "localhost") // note, for multiple elastisearch nodes specify a csv list
    sparkConf.set("es.number_of_shards", "1")
      // Now it's 24 Mb of buffer by default instead of 0.064 Mb
//    sparkConf.set("spark.kryoserializer.buffer.mb","24")

    /*
    https://ogirardot.wordpress.com/2015/01/09/changing-sparks-default-java-serialization-to-kryo/
    spark.kryoserializer.buffer.max.mb (64 Mb by default) : useful if your default buffer size goes further than 64 Mb;
    spark.kryo.referenceTracking (true by default) : c.f. reference tracking in Kryo
    spark.kryo.registrationRequired (false by default) : Kryo’s parameter to define if all serializable classes must be registered
    spark.kryo.classesToRegister (empty string list by default) : you can add a list of the qualified names of all classes that must be registered (c.f. last parameter)
    */
    sparkConf.setAppName("randomNetflowGen")
    // Below line is the hostname or IP address for the driver to listen on. This is used for communicating with the executors and the standalone Master.
//    sparkConf.set("spark.driver.host", "192.168.99.1")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false") // overwrite hdfs files which are written
//
            val jars = Array("/Users/faganpe/.m2/repository/org/apache/spark/spark-streaming-kafka_2.10/1.3.0-cdh5.4.5/spark-streaming-kafka_2.10-1.3.0-cdh5.4.5.jar",
              "/Users/faganpe/.m2/repository/org/apache/kafka/kafka_2.10/0.8.2.0/kafka_2.10-0.8.2.0.jar",
              "/Users/faganpe/.m2/repository/org/apache/spark/spark-core_2.10/1.3.0-cdh5.4.5/spark-core_2.10-1.3.0-cdh5.4.5.jar",
              "/Users/faganpe/.m2/repository/com/101tec/zkclient/0.3/zkclient-0.3.jar",
              "/Users/faganpe/.m2/repository/com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar",
              "/Users/faganpe/.m2/repository/com/esotericsoftware/kryo/kryo/2.21/kryo-2.21.jar",
              "/Users/faganpe/.m2/repository/org/elasticsearch/elasticsearch-spark_2.10/2.1.0.Beta3/elasticsearch-spark_2.10-2.1.0.Beta3.jar",
              "/Users/faganpe/.m2/repository/com/maxmind/db/maxmind-db/1.0.0/maxmind-db-1.0.0.jar",
              "/Users/faganpe/.m2/repository/com/maxmind/geoip2/geoip2/2.1.0/geoip2-2.1.0.jar",
              "/Users/faganpe/.m2/repository/org/apache/spark/spark-hive_2.10/1.3.0-cdh5.4.5/spark-hive_2.10-1.3.0-cdh5.4.5.jar",
              "/Users/faganpe/InteliJProjects/KafkaStreamingPOC/target/netflow-streaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
            //
            sparkConf.setJars(jars)
    //      val ssc = new StreamingContext(sparkConf, Seconds(120))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //    val numRecords: Int = 30000000
//    val numRecords: Int = args(1).toInt
//    val partitions: Int = args(2).toInt
    val hdfsURI = args(0).toString
    println("The application hdfsURI  is: " + hdfsURI)

    val numDirectories = args(3).toInt

    for (dirNum <- 1 to numDirectories) {

      val appRandomDistributionMin = conf.getInt("netflow-app.randomDistributionMin")
      val appRandomDistributionMax = conf.getInt("netflow-app.randomDistributionMax")
      val numPartitions = args(2).toInt
      val countryEnrichment = args(4).toBoolean
      /* Start of working out if we need to randomize or not */

            val recordsPerPartition = {
              if (appRandomDistributionMin == 0 & appRandomDistributionMax == 0) {
                // no randomness to the number of netflow records
                // we are assuming here that numRecords is divisible by partitions, otherwise we need to compensate for the residual
                println("Using the standard number of lines per partition of " + args(1).toInt)
                args(1).toInt
//                BigInt(args(1))
              }
              else {
                val tempRecordsPerPartition = randNum(appRandomDistributionMax - appRandomDistributionMin) + appRandomDistributionMin
                println("Using the randomized number of lines per partition of " + tempRecordsPerPartition)
                tempRecordsPerPartition
              }
            }

      val broadcastVar = sc.broadcast(PopulateRandomString.returnRand()) // returns the csv file as an ArrayBuffer[String]
      val numLinesCSV = broadcastVar.value.length
      val numColsCSV = broadcastVar.value(0).split(",").map(_.trim).length
      val headersCSVStr = PopulateRandomString.headerLine
      val randCSV = scala.util.Random
      val seedRdd = sc.parallelize(Seq[String](), numPartitions).mapPartitions { x => {

        (1 to recordsPerPartition).map { x =>

          var ret_value = ""
          for(i <- 0 until numColsCSV) {
            val lineChosen = broadcastVar.value(r.nextInt(numLinesCSV))
            ret_value = ret_value + "," + lineChosen.split(",")(i)
          }
          ret_value.stripPrefix(",")
          }
        }.iterator
      }

      /* End of working out if we need to randomize or not */
      seedRdd.saveAsTextFile(hdfsURI + "/" + "runNum=" + dirNum)

      /* Start of save to Elasticsearch */

/*      var counter = 1
      val headersCSVList: List[String] =  headersCSVStr.split(",").toList
      val enrichLineES = seedRdd.map(p => {
          val today: Date = Calendar.getInstance().getTime();

          val formatterDate: SimpleDateFormat  = new SimpleDateFormat("yyyy/MM/dd");
          val formatterTime: SimpleDateFormat  = new SimpleDateFormat("hh:mm:ss.SSS");
          val ESDateStr: String = formatterDate.format(today) + " " + formatterTime.format(today)

          var pushRDD: String = "{\"" + headersCSVList(0) + "\"" + " : " + "\"" + p.split(",")(0) + "\"" + " , " + "\"" + "ts" + "\"" + " : " + "\"" + ESDateStr
          for (pos <- 1 until numColsCSV) {
              pushRDD = pushRDD + "\"" + " , " + "\"" + stripChars(headersCSVList(pos), "\"") + "\"" + " : " + "\"" + p.split(",")(pos)

          }
          pushRDD + "\"}"
        })

      try {
        enrichLineES.saveJsonToEs("spark/netflow")
      }
      catch {
//        case ioe: MapperParsingException => ... // more specific cases first !
        case e: Exception => println("Exception occoured!")
      }*/

      /* End of save to Elasticsearch */

    }

  }

} // end of object