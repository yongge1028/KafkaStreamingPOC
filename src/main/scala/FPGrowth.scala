/**
 * Created by faganpe on 22/06/2015.
 */
import com.typesafe.config.ConfigFactory
import org.apache.spark.graphx.Edge
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.{SaveMode, Row}
import org.apache.spark.sql.Row;
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types.{StructType,StructField,StringType,LongType};

object FPGrowth {

  def main(args: Array[String]) {

    val conf = ConfigFactory.load()
    val appName = conf.getString("netflow-app.name")
    val hdfsURI = conf.getString("netflow-app.hdfsURI")
    println("The application name  is: " + appName)

    if (args.length < 4) {
      System.err.println("Usage: " + appName + " <sql> <minsupport> <numpartitions> <dbname>")
      System.err.println("Example: " + appName + " 'select dir, state from rand_netflow' 0.01 2 cloudera")
      System.exit(1)
    }
    else {
      println("Supplied arguments to the program are : " + appName + " " + " " + args(0) + " " + args(1) + " " + args(2) + " " + args(3))
    }

    // setup Variable names
    val argSql = args(0)
    val argMinSupport = args(1).toDouble
    val argNumPartitions = args(2).toInt
    val argDbName = args(3)

    // setup Spark
    val sparkConf = new SparkConf()

    // used for locally testing spark
    sparkConf.setAppName("fpgrowth")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false") // overwrite hdfs files which are written

    /* Star of settings required for locally running Spark on your favourite IDE
     */
    sparkConf.setMaster("local[4]")
    sparkConf.set("spark.driver.host", "10.211.55.1")

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
      "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-mllib_2.10\\1.3.0-cdh5.4.5\\spark-mllib_2.10-1.3.0-cdh5.4.5.jar",
      "D:\\Bowen_Raw_Source\\IntelijProjects\\KafkaStreamingPOC\\target\\netflow-streaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar")

    sparkConf.setJars(jars)

    /* End of settings required for locally running Spark on your favourite IDE
    */

    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // sc is an existing SparkContext.
    val sqlContextHive = new org.apache.spark.sql.hive.HiveContext(sc)
    // set the hive metastore URL for connection to the remote hive metastore
    sqlContextHive.setConf("hive.metastore.uris", "thrift://vm-cluster-node1:9083")

    // Data can easily be extracted from existing sources,
    // such as Apache Hive.
    sqlContextHive.sql("use default")
    sqlContextHive.sql("set hive.mapred.supports.subdirectories=true")
    sqlContextHive.sql("set mapred.input.dir.recursive=true")

    val trainingDataTable = sqlContextHive.sql(argSql)

    val trainingDataTableFPRDD =  trainingDataTable.map(_.toString().split(" ")).cache()

    val model = new FPGrowth()
      .setMinSupport(argMinSupport)
      .setNumPartitions(argNumPartitions)
      .run(trainingDataTableFPRDD)

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }
    println("Number of found frequent item sets : " +model.freqItemsets.count())

    val modelRDD = model.freqItemsets.map(x => (1, x.items.mkString(",") + x.freq))

    // The schema is encoded in a string
    //    val schemaString = "src_ip src_port dest_ip dest_port frequency"
    val schemaString = argSql.split("from")(0).split("select ")(1).replace(",", "") + "frequency"
    println(schemaString)

    // Generate the schema based on the string of the passed in SQL, currently these are all strings
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, false)))

    // Strip the [] and convert to Strings
    val convertToRowRDD = model.freqItemsets.map(x => x.items.mkString(",").stripPrefix("[").stripSuffix("]") + "," + x.freq.toString)

    // create the Spark RDD of type row
    val rowRDD = convertToRowRDD.map(p => Row(p.split(",")(0), p.split(",")(1), p.split(",")(2), p.split(",")(3), p.split(",")(4)))

    // Apply the schema to the RDD and write the data to Hive
    val freqItemsDataFrame = sqlContextHive.createDataFrame(rowRDD, schema)
    freqItemsDataFrame.registerTempTable("tempfreqItemsDataFrame")
    sqlContextHive.sql("drop table freqItemsSaturn")
    sqlContextHive.sql("create table freqItemsSaturn as select * from tempfreqItemsDataFrame")

  } // end main

}
