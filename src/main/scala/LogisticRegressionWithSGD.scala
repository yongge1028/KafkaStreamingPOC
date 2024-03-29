/**
 * Created by faganpe on 22/06/2015.
 */
import com.typesafe.config.ConfigFactory
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.clustering.KMeans
//import org.apache.spark.mllib.fpm.FPGrowth

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
import org.apache.spark.mllib.regression.LabeledPoint

object LogisticRegressionWithSGD {

  def main(args: Array[String]) {

    val conf = ConfigFactory.load()
    val appName = conf.getString("netflow-app.name")
    val hdfsURI = conf.getString("netflow-app.hdfsURI")
    println("The application name  is: " + appName)

//    if (args.length < 4) {
//      System.err.println("Usage: " + appName + " <numRecords> <numFilesPerDir> <numDirectories> <CountryEnrichment>")
//      System.err.println("Example: " + appName + " 30000000 4 10 true")
//      System.exit(1)
//    }
//    else {
//      println("Supplied arguments to the program are : " + appName + " " + " " + args(1).toInt + " " + args(2).toInt + " " + args(3))
//    }

    // setup Spark
    val sparkConf = new SparkConf()
    //    sparkConf.setMaster("local[4]")
//    sparkConf.setMaster("spark://8a0cfaab7088:7077")
//    sparkConf.setMaster("spark://quickstart.cloudera:7077")
    //        sparkConf.setMaster("spark://8a0cfaab7088:7077")
    //    sparkConf.set("spark.executor.memory", "16g")
    //    sparkConf.set("spark.driver.memory", "64g")
    //    sparkConf.set("spark.cores.max", "32")
    sparkConf.setAppName("randomNetflowGen")
    sparkConf.set("spark.hadoop.validateOutputSpecs", "false") // overwrite hdfs files which are written

//    val jars = Array("/Users/faganpe/.m2/repository/org/apache/spark/spark-streaming-kafka_2.10/1.3.0-cdh5.4.1/spark-streaming-kafka_2.10-1.3.0-cdh5.4.1.jar",
//      "/Users/faganpe/.m2/repository/org/apache/kafka/kafka_2.10/0.8.0/kafka_2.10-0.8.0.jar",
//      "/Users/faganpe/.m2/repository/org/apache/spark/spark-core_2.10/1.3.0-cdh5.4.1/spark-core_2.10-1.3.0-cdh5.4.1.jar",
//      "/Users/faganpe/.m2/repository/com/101tec/zkclient/0.3/zkclient-0.3.jar",
//      "/Users/faganpe/.m2/repository/com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar",
//      "/Users/faganpe/.m2/repository/com/esotericsoftware/kryo/kryo/2.21/kryo-2.21.jar",
//      "/Users/faganpe/.m2/repository/org/elasticsearch/elasticsearch-spark_2.10/2.1.0.Beta3/elasticsearch-spark_2.10-2.1.0.Beta3.jar",
//      "/Users/faganpe/.m2/repository/com/maxmind/db/maxmind-db/1.0.0/maxmind-db-1.0.0.jar",
//      "/Users/faganpe/.m2/repository/com/maxmind/geoip2/geoip2/2.1.0/geoip2-2.1.0.jar",
//      "/Users/faganpe/.m2/repository/org/apache/spark/spark-hive_2.10/1.3.0-cdh5.4.1/spark-hive_2.10-1.3.0-cdh5.4.1.jar",
//      "/Users/faganpe/InteliJProjects/KafkaStreamingPOC/target/netflow-streaming-0.0.1-SNAPSHOT-jar-with-dependencies.jar")

//    val jars = Array("/home/faganpe/.m2/repository/org/apache/spark/spark-streaming-kafka_2.10/1.3.0-cdh5.4.0/spark-streaming-kafka_2.10-1.2.0-cdh5.3.0.jar",
//      "/home/faganpe/.m2/repository/org/apache/kafka/kafka_2.10/0.8.0/kafka_2.10-0.8.0.jar",
//      "/home/faganpe/.m2/repository/org/apache/spark/spark-core_2.10/1.2.0-cdh5.3.0/spark-core_2.10-1.3.0-cdh5.4.0.jar",
//      "/home/faganpe/.m2/repository/com/101tec/zkclient/0.3/zkclient-0.3.jar",
//      "/home/faganpe/.m2/repository/com/yammer/metrics/metrics-core/2.2.0/metrics-core-2.2.0.jar",
//      "/home/faganpe/.m2/repository/com/esotericsoftware/kryo/kryo/2.21/kryo-2.21.jar",
//      "/home/faganpe/.m2/repository/org/elasticsearch/elasticsearch-spark_2.10/2.1.0.Beta3/elasticsearch-spark_2.10-2.1.0.Beta3.jar",
//      "/home/faganpe/.m2/repository/com/maxmind/db/maxmind-db/1.0.0/maxmind-db-1.0.0.jar",
//      "/home/faganpe/.m2/repository/com/maxmind/geoip2/geoip2/2.1.0/geoip2-2.1.0.jar",
//      "/home/faganpe/.m2/repository/org/apache/spark/spark-hive_2.10/1.3.0-cdh5.4.0/spark-hive_2.10-1.3.0-cdh5.4.0.jar",
//      "/home/faganpe/.m2/repository/org/apache/hive/shims/hive-shims-0.23/1.1.0-cdh5.4.0/hive-shims-0.23-1.1.0-cdh5.4.0.jar",
//      "/home/faganpe/.m2/repository/org/apache/hive/shims/hive-shims-common/1.1.0-cdh5.4.0/hive-shims-common-1.1.0-cdh5.4.0.jar",
//      "/home/faganpe/.m2/repository/org/apache/hive/shims/hive-shims-scheduler/1.1.0-cdh5.4.0/hive-shims-scheduler-1.1.0-cdh5.4.0.jar",
//      "/home/faganpe/.m2/repository/org/apache/hive/hive-ant/1.1.0-cdh5.4.0/hive-ant-1.1.0-cdh5.4.0.jar",
//      "/home/faganpe/.m2/repository/org/apache/hive/hive-common/1.1.0-cdh5.4.0/hive-common-1.1.0-cdh5.4.0.jar",
//      "/home/faganpe/.m2/repository/org/apache/hive/hive-exec/1.1.0-cdh5.4.0/hive-exec-1.1.0-cdh5.4.0.jar",
//      "/home/faganpe/.m2/repository/org/apache/hive/hive-metastore/1.1.0-cdh5.4.0/hive-metastore-1.1.0-cdh5.4.0.jar",
//      "/home/faganpe/.m2/repository/org/apache/hive/hive-serde/1.1.0-cdh5.4.0/hive-serde-1.1.0-cdh5.4.0.jar",
//      "/home/faganpe/.m2/repository/org/apache/hive/hive-shims/1.1.0-cdh5.4.0/hive-shims-1.1.0-cdh5.4.0.jar",
//      "/home/faganpe/.m2/repository/org/apache/hive/hive-common/1.1.0-cdh5.4.0/hive-common-1.1.0-cdh5.4.0.jar",
//      "/home/faganpe/InteliJProjects/KafkaStreamingPOC/target/netflow-streaming-0.0.1-SNAPSHOT.jar")

    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // sc is an existing SparkContext.
    val sqlContextHive = new org.apache.spark.sql.hive.HiveContext(sc)

    // Data can easily be extracted from existing sources,
    // such as Apache Hive.
    sqlContextHive.sql("use faganp")

//    val trainingDataTable = sqlContextHive.sql("""
//          SELECT dport
//                 stos,
//                 dtos,
//                 totpkts
//          FROM rand_netflow_snappy_sec_stage""")

    val trainingDataTable = sqlContextHive.sql("""
          SELECT stos,
                 dtos,
                 totpkts
          FROM rand_netflow_snappy_sec_stage""")

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
    val parsedData = trainingDataTable.map(row => Vectors.dense(Array(row.getDouble(0), row.getDouble(1))))

    // Cluster the data into two classes using KMeans
    val numIterations = 20
    val numClusters = 2
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)

    println("Within Set Sum of Squared Errors = " + WSSSE)

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
