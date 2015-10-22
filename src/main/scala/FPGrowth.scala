/**
 * Created by faganpe on 22/06/2015.
 */
import com.typesafe.config.ConfigFactory
import org.apache.spark.graphx.Edge
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.sql.Row

//import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD

//import org.apache.spark.sql.types.{StringType, StructField, StructType} // spark 1.3 codeline // spark 1.2 codeline

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
//import util.Properties
import org.apache.spark.mllib.linalg.Vectors


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
    sparkConf.setMaster("local[4]")
//    sparkConf.setMaster("spark://vm-cluster-node2:7077")
//    sparkConf.set("spark.driver.host", "192.168.56.1")
    sparkConf.set("spark.executor.memory", "256m")
    sparkConf.set("spark.driver.memory", "256m")
//    sparkConf.set("spark.cores.max", "4")
//    sparkConf.set("spark.worker.cleanup.enabled", "true")
//    sparkConf.set("spark.worker.cleanup.interval", "1")
//    sparkConf.set("spark.worker.cleanup.appDataTtl", "30")
//    sparkConf.setMaster("spark://quickstart.cloudera:7077")
    //        sparkConf.setMaster("spark://8a0cfaab7088:7077")
    //    sparkConf.set("spark.executor.memory", "16g")
    //    sparkConf.set("spark.driver.memory", "64g")
    //    sparkConf.set("spark.cores.max", "32")
    sparkConf.setAppName("fpgrowth")
//    sparkConf.set("spark.driver.host", "192.168.56.1")
    sparkConf.set("spark.driver.host", "192.168.99.1")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false") // overwrite hdfs files which are written

//    val jars = Array("C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-streaming-kafka_2.10\\1.3.0-cdh5.4.5\\spark-streaming-kafka_2.10-1.3.0-cdh5.4.5.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\kafka\\kafka_2.10\\0.8.0\\kafka_2.10-0.8.0.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-core_2.10\\1.3.0-cdh5.4.5\\spark-core_2.10-1.3.0-cdh5.4.5.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\com\\101tec\\zkclient\\0.3\\zkclient-0.3.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\com\\yammer\\metrics\\metrics-core\\2.2.0\\metrics-core-2.2.0.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\com\\esotericsoftware\\kryo\\kryo\\2.21\\kryo-2.21.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\org\\elasticsearch\\elasticsearch-spark_2.10\\2.1.0.Beta3\\elasticsearch-spark_2.10-2.1.0.Beta3.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\com\\maxmind\\db\\maxmind-db\\1.0.0\\maxmind-db-1.0.0.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\com\\maxmind\\geoip2\\geoip2\\2.1.0\\geoip2-2.1.0.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-hive_2.10\\1.3.0-cdh5.4.5\\spark-hive_2.10-1.3.0-cdh5.4.5.jar",
//      "C:\\Users\\801762473\\.m2\\repository\\org\\apache\\spark\\spark-mllib_2.10\\1.3.0-cdh5.4.5\\spark-mllib_2.10-1.3.0-cdh5.4.5.jar",
//      "D:\\Bowen_Raw_Source\\IntelijProjects\\KafkaStreamingPOC\\target\\netflow-streaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar")
//
//    sparkConf.setJars(jars)

    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // sc is an existing SparkContext.
    val sqlContextHive = new org.apache.spark.sql.hive.HiveContext(sc)
    // set the hive metastore URL for connection to the remote hive metastore
//    sqlContextHive.setConf("hive.metastore.uris", "thrift://vm-cluster-node1:9083")
    sqlContextHive.setConf("hive.metastore.uris", "thrift://192.168.99.102:9083")

    // Data can easily be extracted from existing sources,
    // such as Apache Hive.
    sqlContextHive.sql("use default")
    sqlContextHive.sql("set hive.mapred.supports.subdirectories=true")
    sqlContextHive.sql("set mapred.input.dir.recursive=true")

//    val trainingDataTable = sqlContextHive.sql("""
//          SELECT dport
//                 stos,
//                 dtos,
//                 totpkts
//          FROM rand_netflow_snappy_sec_stage""")

//    val trainingDataTable = sqlContextHive.sql("select dir, state from rand_netflow")
    val trainingDataTable = sqlContextHive.sql(argSql) //.collect().foreach(println)
    trainingDataTable.show()
    // below prints out the SQL RDD
//    val trainingDataTable = sqlContextHive.sql("select srcaddr, dir from rand_netflow limit 100").collect().foreach(println)

//    val transactions: RDD[Array[String]] = ...
//    val transactions = sc.textFile("D:\\Bowen_Raw_Source\\IntelijProjects\\KafkaStreamingPOC\\src\\main\\resources\\sample_fpgrowth.txt").map(_.split(" ")).cache()
//    val transactions = sc.textFile("hdfs://vm-cluster-node1:8020/user/admin/fpgrowth/sample_fpgrowth.txt").map(_.split(" ")).cache()
//    val trainingDataTableFPRDD =  trainingDataTable.map(_.toString().split(" ")).cache()
    val trainingDataTableFPRDD =  trainingDataTable.map(_.toString().split(" ")).cache()

    val model = new FPGrowth()
      .setMinSupport(argMinSupport)
      .setNumPartitions(argNumPartitions)
      .run(trainingDataTableFPRDD)

//    val model = new FPGrowth()
//      .setMinSupport(argMinSupport)
//      .setNumPartitions(argNumPartitions)
//      .run(transactions)
//
    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }
    println("Number of found frequent item sets : " +model.freqItemsets.count())

    // Convert records of the RDD (people) to Rows.
    // Import Row.
    import org.apache.spark.sql.Row

    val rowRDD = model.freqItemsets.map(_.toString.split(" ")).map(p => Row(p(0), p(1).trim))

    // The schema is encoded in a string
    val schemaString = "name age"

    // Import Spark SQL data types
    import org.apache.spark.sql.types.{StructType,StructField,StringType};

    // Generate the schema based on the string of schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // Apply the schema to the RDD.
    val freqPatternDataFrame = sqlContext.createDataFrame(rowRDD, schema)
    freqPatternDataFrame.printSchema()
//    freqPatternDataFrame.show()
//    freqPatternDataFrame.saveAsParquetFile("parquetFPGrowth")

    // Register the DataFrames as a table.
//    freqPatternDataFrame.registerTempTable("rdd_save_table_freqpat")

    // save the table into the hive metastore
//    sqlContextHive.sql("CREATE TABLE IF NOT EXISTS cloudera.hive_rdd_save_table as select  * from rdd_save_table_freqpat")


//    val df = sqlContextHive.createDataFrame(freqItemSetRDD.map() )
//    val freqItemSetRDDPair = freqItemSetRDD.map( x => (1, x))

    // select dport, stos, dtos, totpkts from rand_netflow_snappy_sec_stage limit 10

    // Since `sql` returns an RDD, the results of the above
    // query can be easily used in MLlib
    //    val trainingData = trainingDataTable.map { row =>
    ////      val features = Array([Double](row(1), row(2), row(3))
    //      val features = Vectors.dense(Array(row.getDouble(1), row.getDouble(2), row.getDouble(3)))
    //      LabeledPoint(row.getDouble(0), features)
    ////      val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
    //    }

//    val parsedData = trainingDataTable.map(row => Vectors.dense(Array(row.getDouble(1), row.getDouble(2), row.getDouble(3))))
//    val parsedData = trainingDataTable.map(row => Vectors.dense(Array(row.getDouble(0), row.getDouble(1))))
//
//    // Cluster the data into two classes using KMeans
//    val numIterations = 20
//    val numClusters = 2
//    val clusters = KMeans.train(parsedData, numClusters, numIterations)
//
//    // Evaluate clustering by computing Within Set Sum of Squared Errors
//    val WSSSE = clusters.computeCost(parsedData)
//
//    println("Within Set Sum of Squared Errors = " + WSSSE)

    //    val numIterations = 100
    //    val model = SVMWithSGD.train(trainingData, numIterations)
    //
    //    // Evaluate model on training examples and compute training error
    //    val labelAndPreds = trainingData.map { point =>
    //      val prediction = model.predict(point.features)
    //      (point.label, prediction)
    //    }
    //    val trainErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / trainingData.count
    //    println("Training Error = " + trainErr)

    // Now that we have used SQL to join existing data and train a model, we can use this model to predict which users are likely targets.

    //    val allCandidates = sqlContextHive.sql("""
    //          SELECT proto
    //                 srcaddr,
    //                 dir,
    //                 dstaddr
    //          FROM rand_netflow_snappy_sec_stage""")
    //
    //    // Results of ML algorithms can be used as tables
    //    // in subsequent SQL statements.
    //    case class Score(userId: Int, score: Double)
    //    val scores = allCandidates.map { row =>
    //      val features = Vectors.dense(Array(row.getDouble(1), row.getDouble(2), row.getDouble(3)))
    //      Score(row(0), model.predict(features))
    //    }
    //    scores.registerAsTable("Scores")

  } // end main

}
