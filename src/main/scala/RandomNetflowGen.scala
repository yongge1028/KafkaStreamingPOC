/**
 * Created by faganp on 3/19/15.
 */

import java.io.FileWriter

import Utils.SaveRDD
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.{StringType, StructField, StructType}

//import java.util.Properties

//import _root_.kafka.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import org.apache.hadoop.io.{MapWritable, NullWritable}
//import org.apache.spark.storage.StorageLevel
import org.apache.spark.{streaming, SparkContext, SparkConf}
//import org.apache.spark.serializer.KryoSerializer
//import org.elasticsearch.spark.rdd.EsSpark

//import org.apache.spark.sql.SQLContext
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.Calendar
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
//import util.Properties

/**
 * Created by faganpe on 17/03/15.
 */
object RandomNetflowGen extends Serializable {

  /* Start of the random generation values used to influence the data that is produced */

  val r = scala.util.Random
//  val protoMap = Map(0 -> "udp", 1 -> "tcp", 2 -> "icmp", 3 -> "tcp", 4 -> "tcp")
//  val flowDirMap = Map(0 -> "->", 1 -> "<?>", 2 -> "<->", 3 -> "?>", 4 -> "->", 5 -> "->")
//  val flowStatMap = Map(0 -> "FSPA_FSPA", 1 -> "CON", 2 -> "INT", 3 -> "FA_FA",
//    4 -> "SPA_SPA", 5 -> "S_", 6 -> "URP", 7 -> "CON", 8 -> "CON", 9 -> "CON", 10 -> "CON")
//  //  val ipGenMap = Map(0 -> getIPAddressSkew("132.146.5"), 1 -> getIPRand())
//  val sTosMap = Map(0 -> 0, 1 -> 3, 2 -> 2, 3 -> 2, 4 -> 2)
//  val dTosMap = Map(0 -> 0, 1 -> 3, 2 -> 2, 3 -> 4)
//  val totPktsMap = Map(0 -> randNum(2350), 1 -> randNum(128)) // big and small
//  val totBytesMap = Map(0 -> randNum(128)) // big and small
//  val labelMap = Map(0 -> "flow=From-Botnet-V44-ICMP",
//      1 -> "flow=Backgrund-TCP-Attempt",
//      2 -> "flow=From-Normal-V44-CVUT-WebServer",
//      3 -> "flow=Background-google-analytics14",
//      4 -> "flow=Background-UDP-NTP-Established-1",
//      5 -> "flow=From-Botnet-V44-TCP-CC107-IRC-Not-Encrypted",
//      6 -> "flow=Background-google-analytics4",
//      7 -> "flow=Background-google-analytics9",
//      8 -> "flow=From-Normal-V44-UDP-CVUT-DNS-Server")

  def getIPGenRand(randNum: Int): String = {
    //    val r = scala.util.Random
    if (randNum % 2 == 0) getIPRand()
    else getIPAddressSkew("132.146.5")
    //      getIPAddressSkew("132.146.5")
  }

  /* End of the random generation values used to influence the data that is produced */

  def getIPAddressSkew(IPSubnet: String): String = {
    //    val r = scala.util.Random
    val dotCount = IPSubnet.count(_ == '.') // let's count the number of dots
    if (dotCount == 3) IPSubnet // return the complete IP address without making anything up
    else if (dotCount == 2) IPSubnet + "." + r.nextInt(255)
    else if (dotCount == 1) IPSubnet + "." + r.nextInt(255) + "." + r.nextInt(255)
    else IPSubnet // otherwise just return the original ip string
  }

  def getIPRand(): String = {
    //    val r = scala.util.Random
    InetAddresses.fromInteger(r.nextInt()).getHostAddress()
  }

  def randNum(ranNum: Int): Int = {
    //    val r = scala.util.Random
    r.nextInt(ranNum)
  }

//  def genRandNetflowLine(): String = {
//    //    val r = scala.util.Random
//    val formatDate = new SimpleDateFormat("YYYY/MM/dd HH:MM:ss.SSSSSS")
//    val formatDateDuration = new SimpleDateFormat("ss.SSSSSS")
//    val flowTimestamp = formatDate.format(Calendar.getInstance().getTime())
//    val flowDuration = formatDateDuration.format(Calendar.getInstance().getTime())
//    //    val SourceIPString = InetAddresses.fromInteger(r.nextInt()).getHostAddress()
//    val SourceIPString = getIPGenRand(r.nextInt())
//    val DestIPString = InetAddresses.fromInteger(r.nextInt()).getHostAddress()
//
//    //    println(flowTimestamp + "," + flowDuration + "," + protoMap(r.nextInt(3)) + "," +
//    //      SourceIPString + "," + flowDirMap(r.nextInt(4)) + "," + DestIPString + "," +
//    //      r.nextInt(65535) + "," + flowStatMap(r.nextInt(7)) + "," + sTosMap(r.nextInt(3)) +
//    //      "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
//    //      totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9)))
//    val line = flowTimestamp + "," + flowDuration + "," + protoMap(r.nextInt(5)) + "," +
//      SourceIPString + "," + flowDirMap(r.nextInt(6)) + "," + DestIPString + "," +
//      r.nextInt(65535) + "," + flowStatMap(r.nextInt(11)) + "," + sTosMap(r.nextInt(3)) +
//      "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
//      totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9)) +
//      "," + MaxMindSingleton.getInstance().getCountry(SourceIPString)
//    // include this if using localfile + ","  + Properties.lineSeparator
//    return line
//  }

//  def genRandNetflowLineForSpark(): Unit = {
//    1 to 1000000000 foreach { _ => genRandNetflowLine}
//  }

  def main(args: Array[String]) {

    val conf = ConfigFactory.load()
    val appName = conf.getString("netflow-app.name")
    val hdfsURI = conf.getString("netflow-app.hdfsURI")
    println("The application name  is: " + appName)

    if (args.length < 4) {
      System.err.println("Usage: " + appName + " <numRecords> <numFilesPerDir> <numDirectories> <CountryEnrichment>")
      System.err.println("Example: " + appName + " 30000000 4 10 true")
      System.exit(1)
    }
    else {
      println("Supplied arguments to the program are : " + appName + " " + " " + args(1).toInt + " " + args(2).toInt + " " + args(3))
    }

    val format = new SimpleDateFormat("dd-MM-yyyy-hh-mm-ss")
//    val hdfsPartitionDir = format.format(Calendar.getInstance().getTime())

    // setup Spark
    val sparkConf = new SparkConf()
//    sparkConf.setMaster("local[4]")
//    sparkConf.setMaster("spark://34c74208da48:7077")
//    sparkConf.setMaster("spark://79d4dd97b170:7077")
//    sparkConf.set("spark.executor.memory", "16g")
//    sparkConf.set("spark.driver.memory", "64g")
//    sparkConf.set("spark.cores.max", "32")
    sparkConf.setAppName("randomNetflowGen")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false") // overwrite hdfs files which are written

//    val jars = Array("/home/faganp/.m2/repository/org/apache/spark/spark-streaming-kafka_2.10/1.2.0-cdh5.3.0/spark-streaming-kafka_2.10-1.2.0-cdh5.3.0.jar",
//      "/home/faganp/.m2/repository/org/apache/kafka/kafka_2.10/0.8.0/kafka_2.10-0.8.0.jar",
//      "/home/faganp/.m2/repository/org/apache/spark/spark-core_2.10/1.2.0-cdh5.3.0/spark-core_2.10-1.2.0-cdh5.3.0.jar",
//      "/home/faganp/.m2/repository/com/101tec/zkclient/0.3/zkclient-0.3.jar",
//      "/home/faganp/.m2/repository/com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar",
//      "/home/faganp/.m2/repository/com/esotericsoftware/kryo/kryo/2.21/kryo-2.21.jar",
//      "/home/faganp/.m2/repository/org/elasticsearch/elasticsearch-spark_2.10/2.1.0.Beta3/elasticsearch-spark_2.10-2.1.0.Beta3.jar",
//      "/home/faganp/.m2/repository/com/maxmind/db/maxmind-db/1.0.0/maxmind-db-1.0.0.jar",
//      "/home/faganp/.m2/repository/com/maxmind/geoip2/geoip2/2.1.0/geoip2-2.1.0.jar",
//      "/home/faganp/IntelijProjects/KafkaStreamingPOC/target/sparkwordcount-0.0.1-SNAPSHOT.jar")
//
//    sparkConf.setJars(jars)
    //      val ssc = new StreamingContext(sparkConf, Seconds(120))
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    val numRecords: Int = 30000000
    val numRecords: Int = args(0).toInt
    val partitions: Int = args(1).toInt
    val recordsPerPartition = numRecords / partitions // we are assuming here that numRecords is divisible by partitions, otherwise we need to compensate for the residual

    //  create the for loop for running around this x times
//    for (i <- 1 to args(3).toInt) {
//      val hdfsPartitionDir = format.format(Calendar.getInstance().getTime())
//      val seedRdd = sc.parallelize(Seq.fill(partitions)(recordsPerPartition), partitions)
//      val netflowLine = seedRdd.flatMap(records => Seq.fill(records)(genRandNetflowLine))
//      netflowLine.saveAsTextFile(hdfsURI + "/data/dt=" + hdfsPartitionDir)
//      netflowLine.saveAsTextFile("hdfs://bow-grd-nn-01.bowdev.net/user/faganp/randomNetflow/data/dt=" + hdfsPartitionDir)
//      //        netflowLine.saveAsTextFile("hdfs://bow-grd-nn-01.bowdev.net/user/faganp/randomNetflow/data/" + hdfsPartitionDir)
//    }

    val seedRdd = sc.parallelize(Seq.fill(partitions)(recordsPerPartition), partitions)
    val netflowLine = seedRdd.flatMap(records => Seq.fill(records)("ReplaceWithData"))

    netflowLine.saveAsTextFile(hdfsURI + "/randfile.txt")

    for (i <- 1 to args(2).toInt) {
      println("Loop number : " + i.toString)
      val randLoopLine = sc.textFile(hdfsURI + "/randfile.txt")
      val currentTimeForDirPart = Calendar.getInstance().getTime()

      // start of define hours and mins and maybe secs here
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
      // end of define hours and mins and maybe secs here

      val randLine = randLoopLine.map(x => {
        val r = scala.util.Random

        // start of maps
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
        // end of maps

        // these dates need to be declared here so that the spark worker's work correctly and conform to the values
        // outside of the map
        val formatDate = new SimpleDateFormat("YYYY/MM/dd HH:MM:ss.SSSSSS")
        val formatDateDuration = new SimpleDateFormat("ss.SSSSSS")
        val formatDateDay = new SimpleDateFormat("YYYY-MM-dd")
        val formatDateHour = new SimpleDateFormat("HH")

        // get the current time for flowDuration so we get variability
        val currentTime = Calendar.getInstance().getTime()

        val flowTimestamp = formatDate.format(currentTimeForDirPart)
        val flowDay = formatDateDay.format(currentTimeForDirPart)
        val flowHour = formatDateHour.format(currentTimeForDirPart)
        val flowDuration = formatDateDuration.format(currentTime)
//        val SourceIPString = InetAddresses.fromInteger(r.nextInt()).getHostAddress()
        val SourceIPString = getIPGenRand(r.nextInt())
        val DestIPString = InetAddresses.fromInteger(r.nextInt()).getHostAddress()
        if (args(3).toBoolean) {
          flowTimestamp + ","  + flowDuration + "," + protoMap(r.nextInt(5)) + "," +
            SourceIPString + "," + flowDirMap(r.nextInt(6)) + "," + DestIPString + "," +
            r.nextInt(65535) + "," + flowStatMap(r.nextInt(11)) + "," + sTosMap(r.nextInt(3)) +
            "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
            totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9)) +
            "," + MaxMindSingleton.getInstance().getCountry(SourceIPString)
        }
        else {
          flowTimestamp + ","  + flowDuration + "," + protoMap(r.nextInt(5)) + "," +
            SourceIPString + "," + flowDirMap(r.nextInt(6)) + "," + DestIPString + "," +
            r.nextInt(65535) + "," + flowStatMap(r.nextInt(11)) + "," + sTosMap(r.nextInt(3)) +
            "," + dTosMap(r.nextInt(4)) + "," + totPktsMap(r.nextInt(2)) + "," +
            totBytesMap(r.nextInt(1)) + "," + labelMap(r.nextInt(9))
        }

        // include this if using localfile + ","  + Properties.lineSeparator
      })

      val hdfsPartitionDir = flowDay + "/" + "hour=" + flowHour + "/" + "minute=" + flowMinute + "/" + "second=" + flowSecond // + "/" + "millisecond=" + flowMilliSecond
//      try {
//        randLine.saveAsTextFile(hdfsURI + "/output-random-netflow/" + "dt=" + hdfsPartitionDir)
//      }
//      catch {
//        case e: Exception => println("exception caught in writing to hdfs : " + e);
//        case e: ArrayIndexOutOfBoundsException => println("exception caught in array : " + e);
//      }

      // The schema is encoded in a string, we are currently not using avro
//      val schemaString = "StartTime Dur Proto SrcAddr Dir DstAddr Dport State sTos dTos TotPkts TotBytes Label Country"
      // Generate the schema based on the string of schema
//      val schema =
//        StructType(
//          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

//      val schema = StructType(Array(StructField("StartTime",StringType,true),StructField("Dur",StringType,true),
//        StructField("Proto",StringType,true), StructField("SrcAddr",StringType,true),
//        StructField("Dir",StringType,true), StructField("DstAddr",StringType,true),
//        StructField("Dport",IntegerType,true), StructField("State",StringType,true),
//        StructField("sTos",StringType,true), StructField("dTos",StringType,true),
//        StructField("TotPkts",StringType,true), StructField("TotBytes",StringType,true),
//        StructField("Label",StringType,true), StructField("Country",StringType,true)))

      // Just the true case for now
//      val rowRDD = randLine.map(_.split(",")).map(p => Row(p(0), p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim,
//        p(6).toInt, p(7).trim, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim))

      // Apply the schema to the RDD.
//      val netflowSchemaRDD = sqlContext.applySchema(rowRDD, schema)

//      netflowSchemaRDD.saveAsParquetFile(hdfsURI + "/output-random-netflow/parquetData/" + "dt=" + i.toString + "-"  + hdfsPartitionDir)
      // the function call below SaveRDD.toHive is in the Utils package so we abstract out common functionality
      SaveRDD.toHive(sc, randLine, hdfsPartitionDir, flowDay, flowHour, flowMinute, flowSecond)
//      SaveRDD.toHiveTable(sc, randLine, hdfsPartitionDir, i.toInt, netflowSchemaRDD)
//      SaveRDD.toHiveTable(sc, randLine, hdfsPartitionDir, i.toInt)

//      // sc is an existing SparkContext.
//      val sqlContextHive = new org.apache.spark.sql.hive.HiveContext(sc)
//
//      sqlContextHive.sql("CREATE TABLE IF NOT EXISTS rand_netflow_snappy_sec (StartTime string, "
//        + "Dur string, Proto string, SrcAddr string, Dir string, DstAddr string, "
//        + "Dport tinyint, State string, sTos string, dTos string, TotPkts string, "
//        + "TotBytes string, Label string, Country string) "
//        + "partitioned by (dt string) STORED AS PARQUET "
//        + "location 'hdfs://localhost:8020/user/faganpe/randomNetflow/output-random-netflow/parquetData'")
//
//      sqlContextHive.sql("alter table rand_netflow_snappy_sec add partition (dt='" + i.toString + "-"  +hdfsPartitionDir + "')")

      /*
      CREATE TABLE IF NOT EXISTS rand_netflow_snappy_secs
        (StartTime string,
          Dur string,
          Proto string,
          SrcAddr string,
          Dir string,
          DstAddr string,
          Dport bigint,
          State string,
          sTos string,
          dTos string,
          TotPkts string,
          TotBytes string,
          Label string,
          Country string
          )
      partitioned by (dt string)
      STORED AS PARQUET
      location '/user/faganpe/randomNetflow/output-random-netflow/parquetData'
      */

    }

//    val lines = sc.textFile(hdfsURI + "/randfile.txt")
//    val randLine = lines.map(x => genRandNetflowLine())
//    randLine.saveAsTextFile(hdfsURI + "randNetflow-output")

    //  create the for loop for running around this x times
    //      for (i <- 1 to 10) {
    //        val hdfsPartitionDir = format.format(Calendar.getInstance().getTime())
    //        val seedRdd = ssc.sparkContext.parallelize(Seq.fill(partitions)(recordsPerPartition), partitions)
    //        val netflowLine = seedRdd.flatMap(records => Seq.fill(records)(genRandNetflowLine))
    //        netflowLine.saveAsTextFile("hdfs://bow-grd-nn-01.bowdev.net/user/faganp/randomNetflow/data/" + hdfsPartitionDir)
    //      }

    //    val fw = new FileWriter("/media/101FBBEA0B471A2C/netflowrandom.txt")
    //      val fw = new FileWriter("/home/faganp/randomNetflow/netflowrandom.txt")
    //
    //      try {
    //        1 to 1000000000 foreach { _ => fw.write(genRandNetflowLine) }
    //        //      1 to 1000 foreach { _ => { print(getIPAddressSkew("132.146.5")); println(" " + getIPRand()) } }
    //        //      1 to 1000 foreach { _ => println(getIPGenRand(r.nextInt())) }
    //      }
    //      finally fw.close()

    // 1 to 10 foreach { _ => Files.write(Paths.get("netflowrandom.txt"), genRanNetflowLine.getBytes(StandardCharsets.UTF_8)) }
  }

}