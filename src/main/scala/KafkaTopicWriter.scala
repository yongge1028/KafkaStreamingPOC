import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
 * Created by faganpe on 23/02/15.
 */
object KafkaTopicWriter {

//  def main(args: Array[String]) {
  def run(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    // Zookeeper connection properties
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    // Send some messages
    while(true) {
      val messages = (1 to messagesPerSec.toInt).map { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
          .mkString(" ")

        new KeyedMessage[String, String](topic, str)
      }.toArray

      producer.send(messages: _*)
      Thread.sleep(100)
    }
  }

  def main(args: Array[String]): Unit = {
    val runArgs = Array("localhost:9092", "testin", "2", "5")
    run(runArgs)
  }

}
