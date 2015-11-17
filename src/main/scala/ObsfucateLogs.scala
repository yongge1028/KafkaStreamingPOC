/**
 * Created by faganp on 3/19/15.
 */

import java.io.{BufferedInputStream, FileInputStream}
import java.util.zip.GZIPInputStream

import Utils.{GzFileIterator, PopulateRandomString}
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
import scala.Array._
//import util.Properties
import java.text.SimpleDateFormat;

/**
 * Created by faganpe on 17/03/15.
 */

object ObsfucateLogs extends Serializable {

  // read a gzipped file from disk
//  def gis(s: String): Unit = {
//    gZippedFile = = new GZIPInputStream(new BufferedInputStream(new FileInputStream(s)))
//  }

  val UINDigitTranslation = Map("0" -> "2", "1" -> "3", "2" -> "1", "3" -> "0", "4" -> "4", "5" -> "8", "6" -> "7", "7" -> "6", "8" -> "9", "9" -> "8")

  def UINObsfucate(UIN: String): String = {

    var newUIN = ""
    // test the UIN is 9 digit's long first
    if (UIN.length() == 9) {
      for (c <- UIN) {
        newUIN += UINDigitTranslation(c.toString)
      }
      println("Obsfucated UIN is : " + newUIN)
    }
    else {
      println("UIN does not have a length of 9 digits")
    }
    return newUIN

  }

  def main(args: Array[String]) {

    val conf = ConfigFactory.load()
    val appName = conf.getString("netflow-app.name")
    val proxyUINPos = conf.getString("proxylog.UINPosition")
//    val appRandomDistributionMin = conf.getInt("netflow-app.randomDistributionMin")
//    val appRandomDistributionMax = conf.getInt("netflow-app.randomDistributionMax")
    println("The application name  is: " + appName)

//    if (args.length != 5) {
//      System.err.println("Usage: " + "hdfs://quickstart.cloudera:8020/user/cloudera/randomNetflow <numRecords> <numFilesPerDir> <numDirectories> <CountryEnrichment>")
//      System.err.println("Example: " + "hdfs://quickstart.cloudera:8020/user/cloudera/randomNetflow 30000000 4 10 true")
//      System.exit(1)
//    }
//    else {
//      println("Supplied arguments to the program are : " + args(0).toString + " " + args(1).toInt + " " + args(2).toInt + " " + args(3) + " " + args(4))
//    }

    // setup Spark
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[4]")
//    sparkConf.setMaster("spark://vm-cluster-node2:7077")
//    sparkConf.setMaster("yarn-cluster")
//    sparkConf.setMaster("spark://quickstart.cloudera:7077")
//    sparkConf.setMaster("yarn-client")
//    //    sparkConf.setMaster("spark://79d4dd97b170:7077")
//    sparkConf.set("spark.app.id", "0123456789")
//    sparkConf.set("spark.executor.memory", "256m")
//        sparkConf.set("spark.driver.memory", "256m")
//        sparkConf.set("spark.cores.max", "4")
//    sparkConf.set("spark.worker.cleanup.enabled", "true")
//    sparkConf.set("spark.worker.cleanup.interval", "1")
//    sparkConf.set("spark.worker.cleanup.appDataTtl", "30")

    /* Change to Kyro Serialization */
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // Now it's 24 Mb of buffer by default instead of 0.064 Mb
//    sparkConf.set("spark.kryoserializer.buffer.mb","24")

    /*
    https://ogirardot.wordpress.com/2015/01/09/changing-sparks-default-java-serialization-to-kryo/
    spark.kryoserializer.buffer.max.mb (64 Mb by default) : useful if your default buffer size goes further than 64 Mb;
    spark.kryo.referenceTracking (true by default) : c.f. reference tracking in Kryo
    spark.kryo.registrationRequired (false by default) : Kryo’s parameter to define if all serializable classes must be registered
    spark.kryo.classesToRegister (empty string list by default) : you can add a list of the qualified names of all classes that must be registered (c.f. last parameter)
    */
    sparkConf.setAppName("ObsfucateLogs")
    // Below line is the hostname or IP address for the driver to listen on. This is used for communicating with the executors and the standalone Master.
//    sparkConf.set("spark.driver.host", "192.168.99.1")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false") // overwrite hdfs files which are written
//
//            val jars = Array("C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-streaming-kafka_2.10\\1.3.0-cdh5.4.5\\spark-streaming-kafka_2.10-1.3.0-cdh5.4.5.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\kafka\\kafka_2.10\\0.8.2.0\\kafka_2.10-0.8.2.0.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-core_2.10\\1.3.0-cdh5.4.5\\spark-core_2.10-1.3.0-cdh5.4.5.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\com\\101tec\\zkclient\\0.3\\zkclient-0.3.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\com\\yammer\\metrics\\metrics-core\\2.2.0\\metrics-core-2.2.0.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\com\\esotericsoftware\\kryo\\kryo\\2.21\\kryo-2.21.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\org\\elasticsearch\\elasticsearch-spark_2.10\\2.1.0.Beta3\\elasticsearch-spark_2.10-2.1.0.Beta3.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\com\\maxmind\\db\\maxmind-db\\1.0.0\\maxmind-db-1.0.0.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\com\\maxmind\\geoip2\\geoip2\\2.1.0\\geoip2-2.1.0.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-hive_2.10\\1.3.0-cdh5.4.5\\spark-hive_2.10-1.3.0-cdh5.4.5.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-yarn_2.10\\1.3.0-cdh5.4.5\\spark-yarn_2.10-1.3.0-cdh5.4.5.jar",
//              "C:\\Users\\801762473\\.m2\\repository\\org\\xerial\\snappy\\snappy-java\\1.0.4.1\\snappy-java-1.0.4.1.jar",
//              "D:\\Bowen\\InteliJGit\\KafkaStreamingPOC\\target\\netflow-streaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
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

    // read the gzipped file, eventually we will want to read this into a spark RDD
//    gis("D:\\Bowen\\IntelijGitNewLaptop\\KafkaStreamingPOC\\src\\main\\resources\\bluecoat1.txt.gz")
    val iterator = GzFileIterator(new java.io.File("D:\\Bowen\\IntelijGitNewLaptop\\KafkaStreamingPOC\\src\\main\\resources\\bluecoat1.txt.gz"), "UTF-8")

    while (iterator.hasNext){
      val UIN = iterator.next().split(",")(proxyUINPos.toInt)
      println(iterator.next().split(",")(proxyUINPos.toInt))
      UINObsfucate(iterator.next().split(",")(proxyUINPos.toInt))
    }

//    iterator.foreach(println)

      /* End of working out if we need to randomize or not */
//      seedRdd.saveAsTextFile(hdfsURI + "/" + "runNum=" + dirNum)

  }

}

// end of object