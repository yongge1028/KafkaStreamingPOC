import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
 * Created by faganpe on 23/02/15.
 */
object KafkaReadWrite {

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    //    StreamingExamples.setStreamingLogLevels()

    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaWordCount")
    val ssc =  new StreamingContext(sparkConf, Seconds(20))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    words.print()

    // spit out to Kafka Topic

    //    val wordCounts = words.map(x => (x, 1L))
    //      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(10), Seconds(2), 2)
    //    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
