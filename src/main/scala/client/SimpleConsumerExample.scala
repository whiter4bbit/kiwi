package phi.client

import com.twitter.util.Future

object SimpleConsumerExample {
  def main(args: Array[String]): Unit = {
    val consumer = QueueClient("localhost:8080").simpleConsumer("example-topic", "consumer-1")

    def consume(): Unit = {
      consumer.poll(10).flatMap { batch =>
        batch.iterator.foreach(message => println(new String(message.payload)))
        consumer.offset(batch.offset)
      } ensure consume
    }

    consume()
  }
}
