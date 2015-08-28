/**
 * Created by faganp on 3/19/15.
 */

import java.util.Calendar

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
import java.text.SimpleDateFormat
//import org.elasticsearch.spark._
//import org.apache.spark.SparkContext._
//import org.apache.spark.sql._
//import org.elasticsearch.spark.sql._
import com.google.common.net.InetAddresses
//import util.Properties

/**
 * Created by faganpe on 17/03/15.
 */

object RandomNetflowGenBowen extends Serializable {

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
//    sparkConf.setMaster("spark://192.168.56.102:7077")
    //    sparkConf.setMaster("spark://79d4dd97b170:7077")
        sparkConf.set("spark.executor.memory", "256m")
        sparkConf.set("spark.driver.memory", "256m")
        sparkConf.set("spark.cores.max", "1")
//    sparkConf.set("spark.worker.cleanup.enabled", "true")
//    sparkConf.set("spark.worker.cleanup.interval", "1")
//    sparkConf.set("spark.worker.cleanup.appDataTtl", "30")

    /* Change to Kyro Serialization */
//    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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
      val broadcastVar = sc.broadcast("Hi Paul")
      val seedRdd = sc.parallelize(Seq[String](), numPartitions).mapPartitions { x => {

//        (1 to recordsPerPartition).map { _ =>
        (1 to recordsPerPartition).map { x =>


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
          //          // start of maps

          val event_id = Map( 0 -> "ec00761e-1629-4e64-afb5-8e9f5a625a59#4055",
            1 -> "ec00761e-1629-4e64-afb5-8e9f5a625a59#4087",
            2 -> "14969dea-2cc1-4498-97ce-df198927e868#7365",
            3 -> "f666f69e-d2b8-4ff0-aaaa-4c1284b08a89#238",
            4 -> "14969dea-2cc1-4498-97ce-df198927e868#7402")

          val sensor_id = Map(0 -> 22, 1 -> 73, 2 -> 46, 3 -> 73, 4 -> 73)

          val ts = Map(0 -> "2015-08-19 20:56:41",
            1 -> "2015-08-19 20:56:42",
            2 -> "2015-08-19 20:55:57",
            3 -> "2015-08-19 20:56:02",
            4 -> "2015-08-19 20:55:57")

          val te = Map(0 -> "2015-08-19 20:56:41",
            1 -> "2015-08-19 20:56:42",
            2 -> "2015-08-19 20:55:57",
            3 -> "2015-08-19 20:56:02",
            4 -> "2015-08-19 20:55:57")

          val src_ip = Map(0 -> "147.150.17.77",
            1 -> "147.151.1.240",
            2 -> "74.125.0.57",
            3 -> "147.149.12.168",
            4 -> "147.150.19.100")

          val src_ip_long = Map(0 -> 1369706041,
            1 -> 1149706041,
            2 -> 1249706041,
            3 -> 1149406041,
            4 -> 1245705041)

          val src_port = Map(0 -> 18490, 1 -> 3287, 2 -> 139, 3 -> 55348, 4 -> 22)

          val dst_ip = Map(0 -> "82.146.43.41",
            1 -> "74.125.0.83",
            2 -> "147.149.5.19",
            3 -> "61.158.247.74",
            4 -> "82.146.43.137")

          val dst_ip_long = Map(0 -> 1385311017,
            1 -> 1249706067,
            2 -> 1149406041,
            3 -> 1033828170,
            4 -> 1385311113)

          val dst_port = Map(0 -> 139, 1 -> 139, 2 -> 24167, 3 -> 443, 4 -> 61686)

          val protocol = Map(0 -> "6", 1 -> "6", 2 -> "6", 3 -> "6", 4 -> "6")

          val ip_version = Map(0 -> 4, 1 -> 4, 2 -> 4, 3 -> 4, 4 -> 4)

          val packets = Map(0 -> 8, 1 -> 8, 2 -> 7, 3 -> 1, 4 -> 1)

          val bytes = Map(0 -> 1241, 1 -> 1239, 2 -> 1184, 3 -> 66, 4 -> 66)

          val tcp_flag = Map(0 -> 11, 1 -> 11, 2 -> 11, 3 -> 0, 4 -> 0)

          val tos = Map(0 -> 0, 1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0)

          val traffic_fragmented = Map(0 -> 0, 1 -> 0, 2 -> 0, 3 -> 0, 4 -> 0)

          val sensor_site = Map(0 -> "Alpha Roads Site1",
            1 -> "Delta Postal Site1",
            2 -> "Charlie Water Site1",
            3 -> "Delta Postal Site1",
            4 -> "Delta Postal Site1")

          val sensor_org_name = Map(0 -> "Alpha Roads",
            1 -> "Delta Postal",
            2 -> "Charlie Water",
            3 -> "Delta Postal",
            4 -> "Delta Postal")

          val sensor_org_sector = Map(0 -> "Transport",
            1 -> "Communications",
            2 -> "Water",
            3 -> "Communications",
            4 -> "Communications")

          val sensor_org_type = Map(0 -> "CNI", 1 -> "CNI", 2 -> "CNI", 3 -> "CNI", 4 -> "CNI")

          val sensor_priority = Map(0 -> 3, 1 -> 2, 2 -> 2, 3 -> 2, 4 -> 2)

          val sensor_country = Map(0 -> "UK", 1 -> "UK", 2 -> "UK", 3 -> "UK", 4 -> "UK")

          val sensor_db = Map(0 -> "Mock_sensor_db-01",
            1 -> "Mock_sensor_db-01",
            2 -> "Mock_sensor_db-01",
            3 -> "Mock_sensor_db-01",
            4 -> "Mock_sensor_db-01")

          val geoip_src_country = Map(0 -> "United Kingdom",
            1 -> "United Kingdom",
            2 -> "United States",
            3 -> "United Kingdom",
            4 -> "United Kingdom")

          // Not needed for ""S
//          val geoip_src_subdivisions = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val geoip_src_city = Map(0 -> "", 1 -> "", 2 -> "Mountain View", 3 -> "", 4 -> "")

          val geoip_src_lat = Map(0 -> 51.5,
            1 -> 51.5,
            2 -> 37.419200000000004,
            3 -> 51.5,
            4 -> 51.5)

          val geoip_src_long = Map(0 -> -0.13, 1 -> -0.13, 2 -> -122.0574, 3 -> -0.13, 4 -> -0.13)

          val geoip_src_isp_org = Map(0 -> "BT", 1 -> "BT", 2 -> "Google", 3 -> "BT", 4 -> "BT")

          val geoip_src_as = Map(0 -> 2856, 1 -> 2856, 2 -> 15169, 3 -> 2856, 4 -> 2856)
          
          val geoip_src_as_org = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val geoip_dst_country = Map(0 -> "Russia",
            1 -> "United States",
            2 -> "United Kingdom",
            3 -> "China",
            4 -> "Russia")
          
          val geoip_dst_subdivisions = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val geoip_dst_city = Map(0 -> "Moscow",
            1 -> "Mountain View",
            2 -> "",
            3 -> "Zhengzhou",
            4 -> "Moscow")

          val geoip_dst_lat = Map(0 -> 55.752200000000002,
            1 -> 37.419200000000004,
            2 -> 51.5,
            3 -> 34.683599999999998,
            4 -> 55.752200000000002)

          val geoip_dst_long = Map(0 -> 37.615600000000001,
            1 -> -122.0574,
            2 -> -0.13,
            3 -> 113.5325,
            4 -> 37.615600000000001)

          val geoip_dst_isp_org = Map(0 -> "ISPsystem, cjsc",
            1 -> "Google",
            2 -> "BT",
            3 -> "China Unicom Liaoning",
            4 -> "ISPsystem, cjsc")

          val geoip_dst_as = Map(0 -> 29182, 1 -> 15169, 2 -> 2856, 3 -> 4837, 4 -> 29182)
          
          val geoip_dst_as_org = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val geoip_db = Map(0 -> "2", 1 -> "2", 2 -> "2", 3 -> "2", 4 -> "2")

          val port_src_well_known_service = Map(0 -> "",
            1 -> "directvdata",
            2 -> "netbios-ssn",
            3 -> "",
            4 -> "ssh")

          val port_dst_well_known_service = Map(0 -> "netbios-ssn",
            1 -> "netbios-ssn",
            2 -> "",
            3 -> "https",
            4 -> "")

          val service_db = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val asset_src_site = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val asset_src_org_name = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val asset_src_org_sector = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val asset_src_org_type = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val asset_src_priority = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val asset_src_country = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val asset_dst_site = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val asset_dst_org_name = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val asset_dst_org_sector = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val asset_dst_org_type = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val asset_dst_priority = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val asset_dst_country = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val asset_db = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val threat_src_type = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val threat_src_attacker = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val threat_src_malware = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val threat_src_campaign = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val threat_src_infrastructure = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val threat_dst_type = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val threat_dst_attacker = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val threat_dst_malware = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val threat_dst_campaign = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val threat_dst_infrastructure = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val threat_db = Map(0 -> "MOCK_THREAT_DB_0.3",
            1 -> "MOCK_THREAT_DB_0.3",
            2 -> "MOCK_THREAT_DB_0.3",
            3 -> "MOCK_THREAT_DB_0.3",
            4 -> "MOCK_THREAT_DB_0.3")

          val dredge_id = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val dredge_updated_fields = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val dredge_date = Map(0 -> "", 1 -> "", 2 -> "", 3 -> "", 4 -> "")

          val yyyy = Map(0 -> 2015, 1 -> 2015, 2 -> 2015, 3 -> 2015, 4 -> 2015)

          val mm = Map(0 -> 8, 1 -> 8, 2 -> 8, 3 -> 8, 4 -> 8)

          val dd = Map(0 -> 19, 1 -> 19, 2 -> 19, 3 -> 19, 4 -> 19)
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

          ""

//            if (countryEnrichment) {
//              flowTimestamp + "," + flowDuration + "," + protoMap(r.nextInt(5)) + "," +
//                SourceIPString + "," + flowDirMap(r.nextInt(6)) + "," + DestIPString + "," +
//                r.nextInt(65535) + "," + flowStatMap(r.nextInt(11)) + "," + sTosMap(r.nextInt(3)) +
//                "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
//                totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9)) +
//                "," + MaxMindSingleton.getInstance().getCountry(SourceIPString)
//            }
//            else {
//              event_id(r.nextInt(4)) + "," + flowDuration + "," + protoMap(r.nextInt(5)) + "," +
//                SourceIPString + "," + flowDirMap(r.nextInt(6)) + "," + DestIPString + "," +
//                r.nextInt(65535) + "," + flowStatMap(r.nextInt(11)) + "," + sTosMap(r.nextInt(3)) +
//                "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
//                totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9))
//            }

        }

      }.iterator

      }

      /* End of working out if we need to randomize or not */
      seedRdd.saveAsTextFile(hdfsURI + "/" + "runNum=" + dirNum)
//      seedRdd.saveAsTextFile("randNetflow" + "/" + "runNum=" + dirNum)
    }

  }

}

// end of object
    /* End of new code */
