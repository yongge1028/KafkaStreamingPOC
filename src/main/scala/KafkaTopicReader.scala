import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by faganpe on 23/02/15.
 */
object KafkaTopicReader {

  def main(args: Array[String]) {
      if (args.length < 4) {
        System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
        System.exit(1)
      }

      //    StreamingExamples.setStreamingLogLevels()

      val Array(zkQuorum, group, topics, numThreads) = args
      val sparkConf = new SparkConf().setMaster("local[8]").setAppName("NetflowReader")
      val ssc =  new StreamingContext(sparkConf, Seconds(10))
      // set hdfs check dir
      ssc.checkpoint("checkpoint")

      val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
      val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

      // Filter messages, if the length of the line is 7 put into a new RDD
      val correctlenLines = lines.filter(_.length > 7)

      println("About to print the correct number of lines")
      correctlenLines.print()

      val words = lines.flatMap(_.split(","))

      words.print()


      //    val wordCounts = words.map(x => (x, 1L))
      //      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
      //    wordCounts.print()

      ssc.start()
      ssc.awaitTermination()
    }

}
