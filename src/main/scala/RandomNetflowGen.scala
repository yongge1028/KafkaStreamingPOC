/**
 * Created by faganp on 3/19/15.
 */

import java.io.FileWriter
import java.util

import Utils.{WorkRequest, SaveRDD}
import com.typesafe.config.ConfigFactory
import org.apache.spark.api.java.function.VoidFunction
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Row
 import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.Random

//import org.apache.spark.sql.catalyst.types.{StringType, StructField, StructType} // spark 1.2 codeline

//import java.util.Properties

//import _root_.kafka.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.apache.hadoop.io.{MapWritable, NullWritable}
//import org.apache.spark.storage.StorageLevel
import org.apache.spark.{rdd, streaming, SparkContext, SparkConf}
//import org.apache.spark.serializer.KryoSerializer
//import org.elasticsearch.spark.rdd.EsSpark

//import org.apache.spark.sql.SQLContext
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.{Random, Calendar}
import java.text.SimpleDateFormat
//import org.elasticsearch.spark._
//import org.apache.spark.SparkContext._
//import org.apache.spark.sql._
//import org.elasticsearch.spark.sql._
import com.google.common.net.InetAddresses
import java.nio.file.{Paths, Files}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.hive._
import Array._
//import util.Properties

/**
 * Created by faganpe on 17/03/15.
 */
//class WorkRequest extends Serializable {
//  private var count: Int = 0
//  private var partition: Int = 0
//
//  /* Start of the random generation values used to influence the data that is produced */
////  val r = scala.util.Random
//
//  def this(count: Int, partition: Int) {
//    this()
//    this.count = count
//    this.partition = partition
//  }
//
//  def getCount: Int = {
//    return count
//  }
//
//  def getPartition: Int = {
//    return partition
//  }
//
//  def getIPGenRand(randNum: Int): String = {
//    //    val r = scala.util.Random
//    if (randNum % 2 == 0) getIPRand()
//    else getIPAddressSkew("132.146.5")
//    //      getIPAddressSkew("132.146.5")
//  }
////
////  /* End of the random generation values used to influence the data that is produced */
////
//  def getIPAddressSkew(IPSubnet: String): String = {
//    val r = scala.util.Random
//    val dotCount = IPSubnet.count(_ == '.') // let's count the number of dots
//    if (dotCount == 3) IPSubnet // return the complete IP address without making anything up
//    else if (dotCount == 2) IPSubnet + "." + r.nextInt(255)
//    else if (dotCount == 1) IPSubnet + "." + r.nextInt(255) + "." + r.nextInt(255)
//    else IPSubnet // otherwise just return the original ip string
//  }
//
//  def getIPRand(): String = {
//    val r = scala.util.Random
//    InetAddresses.fromInteger(r.nextInt()).getHostAddress()
//  }
////
//  def randNum(ranNum: Int): Int = {
//    val r = scala.util.Random
//    r.nextInt(ranNum)
//  }
//
//  override def equals(o: Any): Boolean =
//  {
//    if (this == o) return true
//    if (o == null || getClass != o.getClass) return false
//    val that: WorkRequest = o.asInstanceOf[WorkRequest]
//    if (count != that.count) return false
//    if (partition != that.partition) return false
//    return true
//  }
//
//  override def hashCode: Int =
//  {
//    return partition
//  }
//
//  override def toString: String =
//  {
//    var retVal: String = null
//    val r = scala.util.Random
//    var s = ""
//    for (a <- 1 to getCount) {
//      val currentTimeForDirPart = Calendar.getInstance().getTime()
//
//      // these dates need to be declared here so that the spark worker's work correctly and conform to the values
//      // outside of the map
//      val formatDate = new SimpleDateFormat("YYYY-MM-dd HH:MM:ss.SSSSSS")
//      val formatDateDuration = new SimpleDateFormat("ss.SSSSSS")
//      val formatDateDay = new SimpleDateFormat("YYYY-MM-dd")
//      val formatDateHour = new SimpleDateFormat("HH")
//
//      // start of define hours and mins and maybe secs here
//      val formatDateDayForDir = new SimpleDateFormat("YYYY-MM-dd")
//      val formatDateHourForDir = new SimpleDateFormat("HH")
//      val formatDateMinuteForDir = new SimpleDateFormat("mm")
//      val formatDateSecondForDir = new SimpleDateFormat("ss")
//      val formatDateMilliSecondForDir = new SimpleDateFormat("SSS")
//      val flowDay = formatDateDayForDir.format(currentTimeForDirPart)
//      val flowHour = formatDateHourForDir.format(currentTimeForDirPart)
//      val flowMinute = formatDateMinuteForDir.format(currentTimeForDirPart)
//      val flowSecond = formatDateSecondForDir.format(currentTimeForDirPart)
//      val flowMilliSecond = formatDateMilliSecondForDir.format(currentTimeForDirPart)
//      // end of define hours and mins and maybe secs here
//
//      // get the current time for flowDuration so we get variability
//      val currentTime = Calendar.getInstance().getTime()
//
//      val flowTimestamp = formatDate.format(currentTimeForDirPart)
//      val flowDuration = formatDateDuration.format(currentTime)
//
//      val SourceIPString = getIPGenRand(r.nextInt())
//      val DestIPString = InetAddresses.fromInteger(r.nextInt()).getHostAddress()
//
//      // start of maps
//      val protoMap = Map(0 -> "udp", 1 -> "tcp", 2 -> "icmp", 3 -> "tcp", 4 -> "tcp")
//      val flowDirMap = Map(0 -> "->", 1 -> "<?>", 2 -> "<->", 3 -> "?>", 4 -> "->", 5 -> "->")
//      val flowStatMap = Map(0 -> "FSPA_FSPA", 1 -> "CON", 2 -> "INT", 3 -> "FA_FA",
//        4 -> "SPA_SPA", 5 -> "S_", 6 -> "URP", 7 -> "CON", 8 -> "CON", 9 -> "CON", 10 -> "CON")
//      //  val ipGenMap = Map(0 -> getIPAddressSkew("132.146.5"), 1 -> getIPRand())
//      val sTosMap = Map(0 -> 0, 1 -> 3, 2 -> 2, 3 -> 2, 4 -> 2)
//      val dTosMap = Map(0 -> 0, 1 -> 3, 2 -> 2, 3 -> 4)
//      val totPktsMap = Map(0 -> randNum(2350), 1 -> randNum(128)) // big and small
//      val totBytesMap = Map(0 -> randNum(128)) // big and small
//      val labelMap = Map(0 -> "flow=From-Botnet-V44-ICMP",
//          1 -> "flow=Backgrund-TCP-Attempt",
//          2 -> "flow=From-Normal-V44-CVUT-WebServer",
//          3 -> "flow=Background-google-analytics14",
//          4 -> "flow=Background-UDP-NTP-Established-1",
//          5 -> "flow=From-Botnet-V44-TCP-CC107-IRC-Not-Encrypted",
//          6 -> "flow=Background-google-analytics4",
//          7 -> "flow=Background-google-analytics9",
//          8 -> "flow=From-Normal-V44-UDP-CVUT-DNS-Server")
//      // end of maps
//
//      s ++= flowTimestamp + ","  + flowDuration + "," + protoMap(r.nextInt(5)) + "," +
//        SourceIPString + "," + flowDirMap(r.nextInt(6)) + "," + DestIPString + "," +
//        r.nextInt(65535) + "," + flowStatMap(r.nextInt(11)) + "," + sTosMap(r.nextInt(3)) +
//        "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
//        totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9)) + "\n"
//
////      s ++= flowTimestamp + "," + flowDuration + "\n"
//    }
//    return s
//  }
//}

object RandomNetflowGen extends Serializable {

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
//        sparkConf.setMaster("local[4]")
    sparkConf.setMaster("spark://vm-cluster-node2:7077")
    //    sparkConf.setMaster("spark://79d4dd97b170:7077")
        sparkConf.set("spark.executor.memory", "256m")
        sparkConf.set("spark.driver.memory", "256m")
        sparkConf.set("spark.cores.max", "1")
    sparkConf.setAppName("randomNetflowGen")
    // Below line is the hostname or IP address for the driver to listen on. This is used for communicating with the executors and the standalone Master.
    sparkConf.set("spark.driver.host", "192.168.56.1")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false") // overwrite hdfs files which are written

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
//    val recordsPerPartition = {
//      if (appRandomDistributionMin == 0 & appRandomDistributionMax == 0) {
//        // no randomness to the number of netflow records
//        // we are assuming here that numRecords is divisible by partitions, otherwise we need to compensate for the residual
//        println("Using the standard number of lines per partition of " + args(1).toInt)
//        args(1).toInt
//      }
//      else {
//        val tempRecordsPerPartition = randNum(appRandomDistributionMax - appRandomDistributionMin) + appRandomDistributionMin
//        println("Using the randomized number of lines per partition of " + tempRecordsPerPartition)
//        tempRecordsPerPartition
//      }
//    }
//    val recordsPerPartition :Int = 0
//    val recordsPerPartition = args(1).toInt
//    println("Using the number of lines per partition of " + recordsPerPartition)
//    if (appRandomDistributionMin == 0 & appRandomDistributionMax == 0) { // no randomness to the number of netflow records
//      val recordsPerPartition = args(1).toInt // we are assuming here that numRecords is divisible by partitions, otherwise we need to compensate for the residual
//      println("Using the standard number of lines per partition of " + recordsPerPartition)
//    }
//    else {
//      val recordsPerPartition = randNum(appRandomDistributionMax - appRandomDistributionMin) + appRandomDistributionMin
//      println("Using the randomized number of lines per partition of " + recordsPerPartition)
//    }

    /* Start of new code */
//    val numPartitions = args(2).toInt
//    val numDirectories = args(3).toInt
//    val seedRdd = sc.parallelize(Seq[String](), numPartitions).mapPartitions { _ => {
//
//      (1 to recordsPerPartition).map { _ =>
//        val r = scala.util.Random
//        val currentTimeForDirPart = Calendar.getInstance().getTime()
//
//        // start of define hours and mins and maybe secs here
//        val formatDateDayForDir = new SimpleDateFormat("YYYY-MM-dd")
//        val formatDateHourForDir = new SimpleDateFormat("HH")
//        val formatDateMinuteForDir = new SimpleDateFormat("mm")
//        val formatDateSecondForDir = new SimpleDateFormat("ss")
//        val formatDateMilliSecondForDir = new SimpleDateFormat("SSS")
//        val flowDay = formatDateDayForDir.format(currentTimeForDirPart)
//        val flowHour = formatDateHourForDir.format(currentTimeForDirPart)
//        val flowMinute = formatDateMinuteForDir.format(currentTimeForDirPart)
//        val flowSecond = formatDateSecondForDir.format(currentTimeForDirPart)
//        val flowMilliSecond = formatDateMilliSecondForDir.format(currentTimeForDirPart)
//        // end of define hours and mins and maybe secs here
//
//        // start of maps
//        val protoMap = Map(0 -> "udp", 1 -> "tcp", 2 -> "icmp", 3 -> "tcp", 4 -> "tcp")
//        val flowDirMap = Map(0 -> "->", 1 -> "<?>", 2 -> "<->", 3 -> "?>", 4 -> "->", 5 -> "->")
//        val flowStatMap = Map(0 -> "FSPA_FSPA", 1 -> "CON", 2 -> "INT", 3 -> "FA_FA",
//          4 -> "SPA_SPA", 5 -> "S_", 6 -> "URP", 7 -> "CON", 8 -> "CON", 9 -> "CON", 10 -> "CON")
//        //  val ipGenMap = Map(0 -> getIPAddressSkew("132.146.5"), 1 -> getIPRand())
//        val sTosMap = Map(0 -> 0, 1 -> 3, 2 -> 2, 3 -> 2, 4 -> 2)
//        val dTosMap = Map(0 -> 0, 1 -> 3, 2 -> 2, 3 -> 4)
//        val totPktsMap = Map(0 -> randNum(2350), 1 -> randNum(128)) // big and small
//        val totBytesMap = Map(0 -> randNum(128)) // big and small
//        val labelMap = Map(0 -> "flow=From-Botnet-V44-ICMP",
//          1 -> "flow=Backgrund-TCP-Attempt",
//          2 -> "flow=From-Normal-V44-CVUT-WebServer",
//          3 -> "flow=Background-google-analytics14",
//          4 -> "flow=Background-UDP-NTP-Established-1",
//          5 -> "flow=From-Botnet-V44-TCP-CC107-IRC-Not-Encrypted",
//          6 -> "flow=Background-google-analytics4",
//          7 -> "flow=Background-google-analytics9",
//          8 -> "flow=From-Normal-V44-UDP-CVUT-DNS-Server")
//        // end of maps
//
//        val formatDate = new SimpleDateFormat("YYYY-MM-dd HH:MM:ss.SSSSSS")
//        val formatDateDuration = new SimpleDateFormat("ss.SSSSSS")
//        val formatDateDay = new SimpleDateFormat("YYYY-MM-dd")
//        val formatDateHour = new SimpleDateFormat("HH")
//
//        // get the current time for flowDuration so we get variability
//        val currentTime = Calendar.getInstance().getTime()
//
//        val flowTimestamp = formatDate.format(currentTimeForDirPart)
//        //        val flowDay = formatDateDay.format(currentTimeForDirPart)
//        //        val flowHour = formatDateHour.format(currentTimeForDirPart)
//        val flowDuration = formatDateDuration.format(currentTime)
//        //        val SourceIPString = InetAddresses.fromInteger(r.nextInt()).getHostAddress()
//        val SourceIPString = getIPGenRand(r.nextInt())
//        val DestIPString = InetAddresses.fromInteger(r.nextInt()).getHostAddress()
//
//        if (args(4).toBoolean) {
//          flowTimestamp + "," + flowDuration + "," + protoMap(r.nextInt(5)) + "," +
//            SourceIPString + "," + flowDirMap(r.nextInt(6)) + "," + DestIPString + "," +
//            r.nextInt(65535) + "," + flowStatMap(r.nextInt(11)) + "," + sTosMap(r.nextInt(3)) +
//            "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
//            totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9)) +
//            "," + MaxMindSingleton.getInstance().getCountry(SourceIPString)
//        }
//        else {
//          flowTimestamp + "," + flowDuration + "," + protoMap(r.nextInt(5)) + "," +
//            SourceIPString + "," + flowDirMap(r.nextInt(6)) + "," + DestIPString + "," +
//            r.nextInt(65535) + "," + flowStatMap(r.nextInt(11)) + "," + sTosMap(r.nextInt(3)) +
//            "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
//            totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9))
//        }
//
//      }
//
//      }.iterator
//
//    }

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

      val seedRdd = sc.parallelize(Seq[String](), numPartitions).mapPartitions { _ => {

//
//        while(i < args(1).toLong) {
//          val r = scala.util.Random
//          val currentTimeForDirPart = Calendar.getInstance().getTime()
//          //
//          //          // start of define hours and mins and maybe secs here
//          val formatDateDayForDir = new SimpleDateFormat("YYYY-MM-dd")
//          val formatDateHourForDir = new SimpleDateFormat("HH")
//          val formatDateMinuteForDir = new SimpleDateFormat("mm")
//          val formatDateSecondForDir = new SimpleDateFormat("ss")
//          val formatDateMilliSecondForDir = new SimpleDateFormat("SSS")
//          val flowDay = formatDateDayForDir.format(currentTimeForDirPart)
//          val flowHour = formatDateHourForDir.format(currentTimeForDirPart)
//          val flowMinute = formatDateMinuteForDir.format(currentTimeForDirPart)
//          val flowSecond = formatDateSecondForDir.format(currentTimeForDirPart)
//          val flowMilliSecond = formatDateMilliSecondForDir.format(currentTimeForDirPart)
//          //          // end of define hours and mins and maybe secs here
//          //
//          //          // start of maps
//          val protoMap = Map(0 -> "udp", 1 -> "tcp", 2 -> "icmp", 3 -> "tcp", 4 -> "tcp")
//          val flowDirMap = Map(0 -> "->", 1 -> "<?>", 2 -> "<->", 3 -> "?>", 4 -> "->", 5 -> "->")
//          val flowStatMap = Map(0 -> "FSPA_FSPA", 1 -> "CON", 2 -> "INT", 3 -> "FA_FA",
//            4 -> "SPA_SPA", 5 -> "S_", 6 -> "URP", 7 -> "CON", 8 -> "CON", 9 -> "CON", 10 -> "CON")
//          //  val ipGenMap = Map(0 -> getIPAddressSkew("132.146.5"), 1 -> getIPRand())
//          val sTosMap = Map(0 -> 0, 1 -> 3, 2 -> 2, 3 -> 2, 4 -> 2)
//          val dTosMap = Map(0 -> 0, 1 -> 3, 2 -> 2, 3 -> 4)
//          val totPktsMap = Map(0 -> randNum(2350), 1 -> randNum(128)) // big and small
//          val totBytesMap = Map(0 -> randNum(128)) // big and small
//          val labelMap = Map(0 -> "flow=From-Botnet-V44-ICMP",
//              1 -> "flow=Backgrund-TCP-Attempt",
//              2 -> "flow=From-Normal-V44-CVUT-WebServer",
//              3 -> "flow=Background-google-analytics14",
//              4 -> "flow=Background-UDP-NTP-Established-1",
//              5 -> "flow=From-Botnet-V44-TCP-CC107-IRC-Not-Encrypted",
//              6 -> "flow=Background-google-analytics4",
//              7 -> "flow=Background-google-analytics9",
//              8 -> "flow=From-Normal-V44-UDP-CVUT-DNS-Server")
//          //          // end of maps
//          //
//          val formatDate = new SimpleDateFormat("YYYY-MM-dd HH:MM:ss.SSSSSS")
//          val formatDateDuration = new SimpleDateFormat("ss.SSSSSS")
//          val formatDateDay = new SimpleDateFormat("YYYY-MM-dd")
//          val formatDateHour = new SimpleDateFormat("HH")
//          //
//          //          // get the current time for flowDuration so we get variability
//          val currentTime = Calendar.getInstance().getTime()
//          //
//          val flowTimestamp = formatDate.format(currentTimeForDirPart)
//          //        val flowDay = formatDateDay.format(currentTimeForDirPart)
//          //        val flowHour = formatDateHour.format(currentTimeForDirPart)
//          val flowDuration = formatDateDuration.format(currentTime)
//          //        val SourceIPString = InetAddresses.fromInteger(r.nextInt()).getHostAddress()
//          val SourceIPString = getIPGenRand(r.nextInt())
//          val DestIPString = InetAddresses.fromInteger(r.nextInt()).getHostAddress()
//          //
//          if (countryEnrichment) {
//            flowTimestamp + "," + flowDuration + "," + protoMap(r.nextInt(5)) + "," +
//              SourceIPString + "," + flowDirMap(r.nextInt(6)) + "," + DestIPString + "," +
//              r.nextInt(65535) + "," + flowStatMap(r.nextInt(11)) + "," + sTosMap(r.nextInt(3)) +
//              "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
//              totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9)) +
//              "," + MaxMindSingleton.getInstance().getCountry(SourceIPString)
//          }
//          else {
//            flowTimestamp + "," + flowDuration + "," + protoMap(r.nextInt(5)) + "," +
//              SourceIPString + "," + flowDirMap(r.nextInt(6)) + "," + DestIPString + "," +
//              r.nextInt(65535) + "," + flowStatMap(r.nextInt(11)) + "," + sTosMap(r.nextInt(3)) +
//              "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
//              totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9))
//          }
//          i+=1
//        }

//        (1 until args(1).toInt).map { _ =>
        (1 to recordsPerPartition).map { _ =>

            val r = scala.util.Random
            val currentTimeForDirPart = Calendar.getInstance().getTime()
            //
            //          // start of define hours and mins and maybe secs here
            val formatDateDayForDir = new SimpleDateFormat("YYYY-MM-dd")
            val formatDateHourForDir = new SimpleDateFormat("HH")
            val formatDateMinuteForDir = new SimpleDateFormat("mm")
            val formatDateSecondForDir = new SimpleDateFormat("ss")
            val formatDateMilliSecondForDir = new SimpleDateFormat("SSS")
            val flowDay = formatDateDayForDir.format(currentTimeForDirPart)
            val flowHour = formatDateHourForDir.format(currentTimeForDirPart)
            val flowMinute = formatDateMinuteForDir.format(currentTimeForDirPart)
            val flowSecond = formatDateSecondForDir.format(currentTimeForDirPart)
            val flowMilliSecond = formatDateMilliSecondForDir.format(currentTimeForDirPart)
            //          // end of define hours and mins and maybe secs here
            //
            //          // start of maps
            val protoMap = Map(0 -> "udp", 1 -> "tcp", 2 -> "icmp", 3 -> "tcp", 4 -> "tcp")
            val flowDirMap = Map(0 -> "->", 1 -> "<?>", 2 -> "<->", 3 -> "?>", 4 -> "->", 5 -> "->")
            val flowStatMap = Map(0 -> "FSPA_FSPA", 1 -> "CON", 2 -> "INT", 3 -> "FA_FA",
              4 -> "SPA_SPA", 5 -> "S_", 6 -> "URP", 7 -> "CON", 8 -> "CON", 9 -> "CON", 10 -> "CON")
            //  val ipGenMap = Map(0 -> getIPAddressSkew("132.146.5"), 1 -> getIPRand())
            val sTosMap = Map(0 -> 0, 1 -> 3, 2 -> 2, 3 -> 2, 4 -> 2)
            val dTosMap = Map(0 -> 0, 1 -> 3, 2 -> 2, 3 -> 4)
            val totPktsMap = Map(0 -> randNum(2350), 1 -> randNum(128)) // big and small
            val totBytesMap = Map(0 -> randNum(128)) // big and small
            val labelMap = Map(0 -> "flow=From-Botnet-V44-ICMP",
                1 -> "flow=Backgrund-TCP-Attempt",
                2 -> "flow=From-Normal-V44-CVUT-WebServer",
                3 -> "flow=Background-google-analytics14",
                4 -> "flow=Background-UDP-NTP-Established-1",
                5 -> "flow=From-Botnet-V44-TCP-CC107-IRC-Not-Encrypted",
                6 -> "flow=Background-google-analytics4",
                7 -> "flow=Background-google-analytics9",
                8 -> "flow=From-Normal-V44-UDP-CVUT-DNS-Server")
            //          // end of maps
            //
            val formatDate = new SimpleDateFormat("YYYY-MM-dd HH:MM:ss.SSSSSS")
            val formatDateDuration = new SimpleDateFormat("ss.SSSSSS")
            val formatDateDay = new SimpleDateFormat("YYYY-MM-dd")
            val formatDateHour = new SimpleDateFormat("HH")
            //
            //          // get the current time for flowDuration so we get variability
            val currentTime = Calendar.getInstance().getTime()
            //
            val flowTimestamp = formatDate.format(currentTimeForDirPart)
            //        val flowDay = formatDateDay.format(currentTimeForDirPart)
            //        val flowHour = formatDateHour.format(currentTimeForDirPart)
            val flowDuration = formatDateDuration.format(currentTime)
            //        val SourceIPString = InetAddresses.fromInteger(r.nextInt()).getHostAddress()
            val SourceIPString = getIPGenRand(r.nextInt())
            val DestIPString = InetAddresses.fromInteger(r.nextInt()).getHostAddress()
            //
            if (countryEnrichment) {
              flowTimestamp + "," + flowDuration + "," + protoMap(r.nextInt(5)) + "," +
                SourceIPString + "," + flowDirMap(r.nextInt(6)) + "," + DestIPString + "," +
                r.nextInt(65535) + "," + flowStatMap(r.nextInt(11)) + "," + sTosMap(r.nextInt(3)) +
                "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
                totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9)) +
                "," + MaxMindSingleton.getInstance().getCountry(SourceIPString)
            }
            else {
              flowTimestamp + "," + flowDuration + "," + protoMap(r.nextInt(5)) + "," +
                SourceIPString + "," + flowDirMap(r.nextInt(6)) + "," + DestIPString + "," +
                r.nextInt(65535) + "," + flowStatMap(r.nextInt(11)) + "," + sTosMap(r.nextInt(3)) +
                "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
                totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9))
            }

        }

      }.iterator

      }

      /* End of working out if we need to randomize or not */
      seedRdd.saveAsTextFile(hdfsURI + "/" + "runNum=" + dirNum)
//      seedRdd.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/randomNetflow2" + "/" + "runNum=" + dirNum)
//      seedRdd.saveAsTextFile("randomNetflow2")
    }

  }

} // end of object
    /* End of new code */
