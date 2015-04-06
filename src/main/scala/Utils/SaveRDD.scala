package Utils

import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.{StringType, StructField, StructType}

/**
 * Created by faganpe on 31/03/15.
 */
object SaveRDD extends Serializable {

  // This method save's the string RDD to the hdfs directory for partitioned hive tables.
  def toHive(saveSC: SparkContext, rddSaveHive :RDD[String], hdfsPartitionDir: String, hdfsflowDay: String, hdfsflowHour: String, hdfsflowMinute: String, hdfsflowSecond: String): Unit = {

    // setup the application.conf file
    val conf = ConfigFactory.load()
    val hdfsURI = conf.getString("netflow-app.hdfsURI")

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(saveSC)
    val sqlContextHive = new org.apache.spark.sql.hive.HiveContext(saveSC)

    // The schema is encoded in a string, we are currently not using avro
    val schemaString = "StartTime Dur Proto SrcAddr Dir DstAddr Dport State sTos dTos TotPkts TotBytes Label"
    // Generate the schema based on the string of schema
    //      val schema =
    //        StructType(
    //          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val schema = StructType(Array(StructField("StartTime",StringType,true),StructField("Dur",StringType,true),
      StructField("Proto",StringType,true), StructField("SrcAddr",StringType,true),
      StructField("Dir",StringType,true), StructField("DstAddr",StringType,true),
      StructField("Dport",IntegerType,true), StructField("State",StringType,true),
      StructField("sTos",StringType,true), StructField("dTos",StringType,true),
      StructField("TotPkts",StringType,true), StructField("TotBytes",StringType,true),
      StructField("Label",StringType,true)))

    // Just the true case for now
    val rowRDD = rddSaveHive.map(_.split(",")).map(p => Row(p(0), p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim,
      p(6).toInt, p(7).trim, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim))

    // Apply the schema to the RDD.
    val netflowSchemaRDD = sqlContext.applySchema(rowRDD, schema)

    // Save the parquet file
    netflowSchemaRDD.saveAsParquetFile(hdfsURI + "/output-random-netflow/parquetData/" + "dt=" + hdfsPartitionDir)

    try {
//      sqlContextHive.sql("CREATE DATABASE IF NOT EXISTS faganpe")

      sqlContextHive.sql("use faganpe")

      sqlContextHive.sql("CREATE TABLE IF NOT EXISTS rand_netflow_snappy_sec (StartTime string, "
        + "Dur string, Proto string, SrcAddr string, Dir string, DstAddr string, "
        + "Dport tinyint, State string, sTos string, dTos string, TotPkts string, "
        + "TotBytes string, Label string) "
        + "partitioned by (dt string, hour string, minute string, second string) STORED AS PARQUET "
        + "location " + "'" + hdfsURI + "/output-random-netflow/parquetData'")

      sqlContextHive.sql("alter table rand_netflow_snappy_sec add partition (dt='" + hdfsflowDay + "'" + "," + "hour='" + hdfsflowHour + "'" + "," + "minute='" + hdfsflowMinute + "'" + "," + "second='" + hdfsflowSecond + "')")
    }
    catch {
      case e: Exception => println("exception caught in writing to hdfs : " + e);
      case e: ArrayIndexOutOfBoundsException => println("exception caught in array : " + e);
    }

  }

  // This method save's the string RDD to the hdfs directory for partitioned hive tables.
  def toHiveTable (saveSC: SparkContext, rddSaveHive :RDD[String], hdfsPartitionDir: String, Prefix: Int): Unit = {

    // setup the application.conf file
    val conf = ConfigFactory.load()
    val hdfsURI = conf.getString("netflow-app.hdfsURI")

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(saveSC)
    val sqlContextHive = new org.apache.spark.sql.hive.HiveContext(saveSC)

    // The schema is encoded in a string, we are currently not using avro
    val schemaString = "StartTime Dur Proto SrcAddr Dir DstAddr Dport State sTos dTos TotPkts TotBytes Label Country flowDay flowHour"
    // Generate the schema based on the string of schema
    //      val schema =
    //        StructType(
    //          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    val schema = StructType(Array(StructField("StartTime",StringType,true), StructField("Dur",StringType,true),
      StructField("Proto",StringType,true), StructField("SrcAddr",StringType,true),
      StructField("Dir",StringType,true), StructField("DstAddr",StringType,true),
      StructField("Dport",IntegerType,true), StructField("State",StringType,true),
      StructField("sTos",StringType,true), StructField("dTos",StringType,true),
      StructField("TotPkts",StringType,true), StructField("TotBytes",StringType,true),
      StructField("Label",StringType,true), StructField("Country",StringType,true),
      StructField("flowDay",StringType,true), StructField("flowHour",StringType,true)))

    // Just the true case for now
    val rowRDD = rddSaveHive.map(_.split(",")).map(p => Row(p(0), p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim, p(6).toInt, p(7).trim,
      p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim))

    // Apply the schema to the RDD.
    val netflowSchemaRDD = sqlContext.applySchema(rowRDD, schema).registerTempTable("parquetTempTable")

    try {
      //      sqlContextHive.sql("CREATE DATABASE IF NOT EXISTS faganpe")

      sqlContextHive.sql("use faganpe")

      sqlContextHive.sql("CREATE TABLE IF NOT EXISTS rand_netflow_snappy_sec (StartTime string, "
        + "Dur string, Proto string, SrcAddr string, Dir string, DstAddr string, "
        + "Dport tinyint, State string, sTos string, dTos string, TotPkts string, "
        + "TotBytes string, Label string, Country string) "
        + "partitioned by (flowDay string, flowHour string) STORED AS PARQUET ")
//        + "location " + "'" + hdfsURI + "/output-random-netflow/parquetData'")

//      sqlContextHive.sql("alter table rand_netflow_snappy_sec add partition (dt='" + Prefix.toString + "-" + hdfsPartitionDir + "')")

      // see - http://stackoverflow.com/questions/25484879/sql-over-spark-streaming

//      val insertPar = schemeRDD.saveAsParquetFile("hdfs://localhost:8020/user/faganpe/test")
//      val parquetFile = sqlContext.parquetFile("hdfs://localhost:8020/user/faganpe/test")
//      // we can use the parquet file as a temp table, so actually we only need to write the parquet file not the table above
//      parquetFile.registerTempTable("parquetFile")
      // now comes our insert statement
//      val insertPar = sqlContext.sql("INSERT INTO TABLE rand_netflow_snappy_sec PARTITION (dt) select Country, Dur, StartTime FROM parquetTempTable")
      sqlContextHive.sql("use faganpe")
//      val insertPar = sqlContext.sql("INSERT OVERWRITE INTO rand_netflow_snappy_sec PARTITION (flowDay, flowHour) SELECT StartTime, Dur, Proto, SrcAddr, Dir, DstAddr, Dport, State, sTos, dTos, TotPkts, TotBytes, Label, Country, flowDay, flowHour FROM parquetTempTable")
    }
    catch {
      case e: Exception => println("exception caught error : " + e);
      case e: ArrayIndexOutOfBoundsException => println("exception caught in array : " + e);
    }

  }

}

//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.StreamingContext._
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.streaming.Duration
//
//object StreamingSQL {
//
//  case class Persons(name: String, age: Int)
//
//  def main(args: Array[String]) {
//
//    val sparkConf = new SparkConf().setMaster("local").setAppName("HdfsWordCount")
//    val sc = new SparkContext(sparkConf)
//    // Create the context
//    val ssc = new StreamingContext(sc, Seconds(2))
//
//    val lines = ssc.textFileStream("C:/Users/pravesh.jain/Desktop/people/")
//    lines.foreachRDD(rdd=>rdd.foreach(println))
//
//    val sqc = new SQLContext(sc);
//    import sqc.createSchemaRDD
//
//    // Create the FileInputDStream on the directory and use the
//    // stream to count words in new files created
//
//    lines.foreachRDD(rdd=>{
//      rdd.map(_.split(",")).map(p => Persons(p(0), p(1).trim.toInt)).registerAsTable("data")
//      val teenagers = sqc.sql("SELECT name FROM data WHERE age >= 13 AND age <= 19")
//      teenagers.foreach(println)
//    })
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
