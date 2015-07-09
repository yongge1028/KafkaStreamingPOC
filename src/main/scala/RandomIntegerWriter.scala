/**
 * Created by faganpe on 29/06/2015.
 */

import util.Random

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object RandomIntegerWriter {
  def main(args: Array[String]) {
//    if (args.length < 2) {
//      System.err.println("Usage: RandomIntegerWriter <num Integers> <outDir>")
//      System.exit(1)
//    }
    val sparkConf = new SparkConf().setAppName("Spark RandomIntegerWriter")
    sparkConf.setMaster("local[4]")
    val spark = new SparkContext(sparkConf)
    val numPartitions = 4
    val recordsPerPartition = 1000
//    val distData = spark.parallelize(Seq.fill(args(0).toInt)(Random.nextInt))
    val distData = spark.parallelize(Seq[Int](), numPartitions)
      .mapPartitions { _ => {
      (1 to recordsPerPartition).map{_ => Random.nextInt}.iterator
    }}
//    distData.saveAsTextFile(args(1))
    distData.saveAsTextFile("RandomIntegerWriter")
    spark.stop()
  }
}