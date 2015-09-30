import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkContext, SparkConf}
//import parquet.hadoop.ParquetInputFormat
import parquet.hadoop.{ParquetOutputFormat, ParquetInputFormat}
// First we're going to import the classes we need
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.avro.generic.GenericRecord
import parquet.hadoop.ParquetInputFormat
import parquet.avro.AvroReadSupport
import org.apache.spark.rdd.RDD

/**
 * Created by 801762473 on 28/09/2015.
 */
object readParquet extends Serializable {

  // Then we create RDD's for 2 of the files we imported from MySQL with Sqoop
  // RDD's are Spark's data structures for working with distributed datasets
  def rddFromParquetHdfsFile(path: String, sc: SparkContext): RDD[GenericRecord] = {
    val job = new Job()
    FileInputFormat.setInputPaths(job, path)
    ParquetInputFormat.setReadSupportClass(job,
      classOf[AvroReadSupport[GenericRecord]])
    return sc.newAPIHadoopRDD(job.getConfiguration,
      classOf[ParquetInputFormat[GenericRecord]],
      classOf[Void],
      classOf[GenericRecord]).map(x => x._2)
  }

  def main(args: Array[String]): Unit = {

    // setup Spark
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[4]")
    sparkConf.setAppName("readParquet")

    val sc = new SparkContext(sparkConf)

//    val readFile = rddFromParquetHdfsFile("hdfs://vm-cluster-node1:8020/user/admin/bowen/", sc)
    val readFile = rddFromParquetHdfsFile("hdfs://vm-cluster-node1:8020/user/admin/bowen-nostring", sc)
//    val readFile = rddFromParquetHdfsFile("D:\\Bowen_Raw_Source\\IntelijProjects\\KafkaStreamingPOC\\src\\main\\resources\\8f4971d24ba0216e-af9f915ef1277490_1753085081_data.0.parq", sc)
    readFile.take(10).foreach(println)

  }

}
