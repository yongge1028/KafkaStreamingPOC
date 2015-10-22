/**
 * Created by faganp on 3/19/15.
 */

import java.io.FileWriter
import java.util

import Utils.{NetFlowDef, PopulateRandomString, WorkRequest, SaveRDD}
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
import org.elasticsearch.spark._
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Calendar;

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
//    sparkConf.setMaster("spark://quickstart.cloudera:7077")
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
//    sparkConf.set("spark.driver.host", "192.168.56.1")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false") // overwrite hdfs files which are written
//
//            val jars = Array("C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-streaming-kafka_2.10\\1.3.0-cdh5.4.5\\spark-streaming-kafka_2.10-1.3.0-cdh5.4.5.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\kafka\\kafka_2.10\\0.8.0\\kafka_2.10-0.8.0.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-core_2.10\\1.3.0-cdh5.4.5\\spark-core_2.10-1.3.0-cdh5.4.5.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\com\\101tec\\zkclient\\0.3\\zkclient-0.3.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\com\\yammer\\metrics\\metrics-core\\2.2.0\\metrics-core-2.2.0.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\com\\esotericsoftware\\kryo\\kryo\\2.21\\kryo-2.21.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\org\\elasticsearch\\elasticsearch-spark_2.10\\2.1.0.Beta3\\elasticsearch-spark_2.10-2.1.0.Beta3.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\com\\maxmind\\db\\maxmind-db\\1.0.0\\maxmind-db-1.0.0.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\com\\maxmind\\geoip2\\geoip2\\2.1.0\\geoip2-2.1.0.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-hive_2.10\\1.3.0-cdh5.4.5\\spark-hive_2.10-1.3.0-cdh5.4.5.jar",
//              "D:\\Bowen_Raw_Source\\IntelijProjects\\KafkaStreamingPOC\\target\\netflow-streaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
//            //
//            sparkConf.setJars(jars)
    //      val ssc = new StreamingContext(sparkConf, Seconds(120))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //    val numRecords: Int = 30000000
//    val numRecords: Int = args(1).toInt
//    val partitions: Int = args(2).toInt
    val hdfsURI = args(0).toString
    println("The application hdfsURI  is: " + hdfsURI)

    // Start of Illustrate a point
    // Excellent source of examples (not an e.g. for everything though) - http://homepage.cs.latrobe.edu.au/zhe/ZhenHeSparkRDDAPIExamples.html
//    val testRDD = sc.textFile(hdfsURI + "/a_txt_file.txt") // testRDD is of RDD type RDD[String]
//    // we could write this which would be the same thing but no implicit datatype is assumed - dynamic typing
//    val testRDD2: RDD[String] = sc.textFile(hdfsURI + "/a_txt_file.txt") // type is included i.e. static typing
//    // Let's convert testRDD to a PairRDD we do this with a map function
//    val testPairRDD = testRDD.map( x => (x, 1)) // split the RDD elements by a space
//    // now we can use a PairRDD function on our new PairRDD and we are also triggering an action to run as partof the DAG
//    // DAG = Directed Acyclic Graph a.k.a Execution Engine
//    // http://www.quora.com/As-it-is-mentioned-Apache-Spark-follows-a-DAG-Directed-Acyclic-Graph-execution-engine-for-execution-What-is-the-whole-concept-about-it-and-the-overall-architecture-of-the-Spark
//    testPairRDD.saveAsHadoopFile(hdfsURI)
//    // End of Illustrate a point

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

//      val seedRdd = sc.parallelize(Seq[String](), numPartitions).mapPartitions { _ => {
      val broadcastVar = sc.broadcast(PopulateRandomString.returnRand()) // returns the csv file as an ArrayBuffer[String]
      val numLinesCSV = broadcastVar.value.length
      val numColsCSV = broadcastVar.value(0).split(",").map(_.trim).length
      val headersCSVStr = PopulateRandomString.headerLine
      val randCSV = scala.util.Random
//      val numLinesCSV = PopulateRandomString.numLines()
      val seedRdd = sc.parallelize(Seq[String](), numPartitions).mapPartitions { x => {

//        (1 to recordsPerPartition).map { _ =>
        (1 to recordsPerPartition).map { x =>

          /*
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
          PopulateRandomString.returnRand()
          */
//            if (countryEnrichment) {
//              flowTimestamp + "," + flowDuration + "," + protoMap(r.nextInt(5)) + "," +
//                SourceIPString + "," + flowDirMap(r.nextInt(6)) + "," + DestIPString + "," +
//                r.nextInt(65535) + "," + flowStatMap(r.nextInt(11)) + "," + sTosMap(r.nextInt(3)) +
//                "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
//                totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9)) +
//                "," + MaxMindSingleton.getInstance().getCountry(SourceIPString)
//            }
//            else {
//              flowTimestamp + "," + flowDuration + "," + protoMap(r.nextInt(5)) + "," +
//                SourceIPString + "," + flowDirMap(r.nextInt(6)) + "," + DestIPString + "," +
//                r.nextInt(65535) + "," + flowStatMap(r.nextInt(11)) + "," + sTosMap(r.nextInt(3)) +
//                "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
//                totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9))
//            }

//          val numLinesCSV = broadcastVar.value.length
//          val numColsCSV = broadcastVar.value(0).split(",").map(_.trim).length

          var ret_value = ""
          for(i <- 0 until numColsCSV) {
//            println("Building column number : " + i)
            val lineChosen = broadcastVar.value(r.nextInt(numLinesCSV))
            ret_value = ret_value + "," + lineChosen.split(",")(i)
          }

//          println("Adding line : " + ret_value.stripPrefix(","))
          ret_value.stripPrefix(",")

//          PopulateRandomString.returnRand()
        }

      }.iterator

      }

      /* End of working out if we need to randomize or not */
      seedRdd.saveAsTextFile(hdfsURI + "/" + "runNum=" + dirNum)
//      seedRdd.saveAsTextFile("runNum=" + dirNum)

      /* Start of save to Elasticsearch */

      var counter = 1
//      val headersCSVStr = "sensor_id,ts,te,duration,src_ip,src_port,dest_ip,dest_port,protocol,ip_version,packets,bytes,tcp_flag_a,tcp_flag_s,tcp_flag_f,tcp_flag_r,tcp_flag_p,tcp_flag_u,tos,reason_for_flow,sensor_site,sensor_org_name,sensor_org_sector,sensor_org_type,sensor_priority,sensor_country,geoip_src_country,geoip_src_subdivisions,geoip_src_city,geoip_src_lat,geoip_src_long,geoip_src_isp_org,geoip_src_as,geoip_src_as_org,geoip_dst_country,geoip_dst_subdivisions,geoip_dst_city,geoip_dst_lat,geoip_dst_long,geoip_dst_isp_org,geoip_dst_as,geoip_dst_as_org,port_src_well_known_service,port_dst_well_known_service,asset_src_site,asset_src_org_name,asset_src_org_sector,asset_src_org_type,asset_src_priority,asset_src_country,asset_dst_site,asset_dst_org_name,asset_dst_org_sector,asset_dst_org_type,asset_dst_priority,asset_dst_country,threat_src_type,threat_src_attacker,threat_src_malware,threat_src_campaign,threat_src_infrastructure,threat_dst_type,threat_dst_attacker,threat_dst_malware,threat_dst_campaign,threat_dst_infrastructure,yyyy,mm,dd,hh,mi"
      val headersCSVList: List[String] =  headersCSVStr.split(",").toList
//      val numOfCSVEntries = headersCSVList.length
        val enrichLineES = seedRdd.map(p => {
//          var pushRDD: String = "{\"" + headersCSVList(0) + "\"" + " : " + "\"" + p.split(",")(0) // + "\"" + " , "
          // add an ES TZ so we can use kibana ok
//          val ESDateStr = headersCSVList(1).split(" ")(0) + "T" + headersCSVList(1).split(" ")(1) + "Z"
//          val ESDateStr = "2015/04/30T18:20:43Z"
          // (1) get today's date
          val today: Date = Calendar.getInstance().getTime();

          // (2) create a date "formatter" (the date format we want)
          val formatterDate: SimpleDateFormat  = new SimpleDateFormat("yyyy/MM/dd");
          val formatterTime: SimpleDateFormat  = new SimpleDateFormat("hh:mm:ss.SSS");
//          val ESDateStr: String = formatterDate.format(today) + "T" + formatterTime.format(today) + "Z"
          val ESDateStr: String = formatterDate.format(today) + " " + formatterTime.format(today)

          var pushRDD: String = "{\"" + headersCSVList(0) + "\"" + " : " + "\"" + p.split(",")(0) + "\"" + " , " + "\"" + "ts" + "\"" + " : " + "\"" + ESDateStr
          for (pos <- 1 until numColsCSV) {
            //           build the ES json RDD string
//            pushRDD = pushRDD + "\"" + " , " + "\"Dur\" : " + "\"" + p.split(",")(pos)
//              pushRDD = pushRDD + "\"" + " , " + "\"" + headersCSVList(pos) + "\"" + " : " + "\"" + p.split(",")(pos)
              // currently we need to strip out the " characters from the csv file and replace with '
              pushRDD = pushRDD + "\"" + " , " + "\"" + stripChars(headersCSVList(pos), "\"") + "\"" + " : " + "\"" + p.split(",")(pos)

            // Below is an example of an ES Json string that works

            //          "{\"StartTime\" : " + "\"" + p.split(",")(0) + "\"" + " , " +
            //            "\"Dur\" : " + "\"" + p.split(",")(1) + "\"" + " , " +
            //            "\"Label12\" : " + "\"" + p.split(",")(24) + "\"" + " , " +
            //            "\"Country\" : " + "\"" + p.split(",")(3) + "\"}"

          }
          // return pushRDD and last closing string to close off the ES json
          pushRDD + "\"}"
        })

      try {
        enrichLineES.saveJsonToEs("spark/netflow")
      }
      catch {
//        case ioe: MapperParsingException => ... // more specific cases first !
        case e: Exception => println("Exception occoured!")
      }

      /* End of save to Elasticsearch */

//      seedRdd.saveAsTextFile("randNetflow" + "/" + "runNum=" + dirNum)
    }

  }

} // end of object
    /* End of new code */
