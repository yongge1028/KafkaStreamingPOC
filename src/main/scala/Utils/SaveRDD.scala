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
  def toHive(saveSC: SparkContext, rddSaveHive :RDD[String], hdfsPartitionDir: String, Prefix: Int): Unit = {

    // setup the application.conf file
    val conf = ConfigFactory.load()
    val hdfsURI = conf.getString("netflow-app.hdfsURI")

    // sc is an existing SparkContext.
    val sqlContext = new org.apache.spark.sql.SQLContext(saveSC)
    val sqlContextHive = new org.apache.spark.sql.hive.HiveContext(saveSC)

    // The schema is encoded in a string, we are currently not using avro
    val schemaString = "StartTime Dur Proto SrcAddr Dir DstAddr Dport State sTos dTos TotPkts TotBytes Label Country"
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
      StructField("Label",StringType,true), StructField("Country",StringType,true)))

    // Just the true case for now
    val rowRDD = rddSaveHive.map(_.split(",")).map(p => Row(p(0), p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim,
      p(6).toInt, p(7).trim, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim))

    // Apply the schema to the RDD.
    val netflowSchemaRDD = sqlContext.applySchema(rowRDD, schema)

    try {
      sqlContextHive.sql("CREATE DATABASE IF NOT EXISTS faganpe")

      sqlContextHive.sql("use faganpe; CREATE TABLE IF NOT EXISTS rand_netflow_snappy_sec (StartTime string, "
        + "Dur string, Proto string, SrcAddr string, Dir string, DstAddr string, "
        + "Dport tinyint, State string, sTos string, dTos string, TotPkts string, "
        + "TotBytes string, Label string, Country string) "
        + "partitioned by (dt string) STORED AS PARQUET "
        + "location " + "'" + hdfsURI + "/output-random-netflow/parquetData'")

      sqlContextHive.sql("use faganpe; alter table rand_netflow_snappy_sec add partition (dt='" + Prefix.toString + "-" + hdfsPartitionDir + "')")
    }
    catch {
      case e: Exception => println("exception caught in writing to hdfs : " + e);
    }

  }

}
