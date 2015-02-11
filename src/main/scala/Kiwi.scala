package phi

import java.nio.file.{Path => JPath}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.HashMap

import com.twitter.conversions.storage._
import com.twitter.util.StorageUnit

import Exceptions._

import phi.io._

class Kiwi private (baseDir: JPath, maxSegmentSize: StorageUnit = 500 megabytes,
    logFlushIntervalMessages: Int = 1000, offsetFlushIntervalUpdates: Int = 1000) extends Logger {

  private val logs = CachedResource((topic: String) => Log.open(baseDir, topic, maxSegmentSize, logFlushIntervalMessages))
  private val offsetStorages = CachedResource((topic: String) => LogOffsetStorage.open(baseDir, topic, offsetFlushIntervalUpdates))
  private val producers = CachedResource((topic: String) => new Producer(logs.get(topic)))
  private val globalConsumers = CachedResource((topic: String) => new GlobalConsumer(logs.get(topic), offsetStorages.get(topic)))
  private val offsetConsumers = CachedResource { p: (String, String) =>
    val (topic, consumer) = p
    new OffsetConsumer(logs.get(topic), consumer, offsetStorages.get(topic))
  }
  private val awaitableConsumer = AwaitableConsumer.start(this)

  private def init(): Unit = {
    log.info("Starting Kiwi")

    if (!baseDir.exists) {
      baseDir.createDirectories
    }

    baseDir.listFiles(_.isDirectory).foreach { topicDir =>
      val topicName = topicDir.toFile.getName()
      log.info("Loading topic: %s", topicName)
      logs.get(topicName)
    }
  }

  def getConsumer(topic: String): Consumer = {
    globalConsumers.get(topic)
  }

  def getConsumer(topic: String, consumer: String): Consumer = {
    offsetConsumers.get(topic, consumer)
  }

  def getOffsetStorage(topic: String): LogOffsetStorage = {
    offsetStorages.get(topic)
  }

  def getProducer(topic: String): Producer = {
    producers.get(topic)
  }

  def getAwaitableConsumer(): AwaitableConsumer = {
    awaitableConsumer
  }

  def shutdown(): Unit = {
    logs.getAll.foreach { log =>
      swallow(log.close)
    }
    offsetStorages.getAll.foreach { storage =>
      swallow(storage.close)
    }
  }
}

object Kiwi {
  def start(baseDir: JPath, maxSegmentSize: StorageUnit = 500 megabytes, logFlushIntervalMessages: Int = 1000, offsetFlushIntervalUpdates: Int = 1000) = {
    val kiwi = new Kiwi(baseDir, maxSegmentSize, logFlushIntervalMessages, offsetFlushIntervalUpdates)
    kiwi.init
    kiwi
  }
}
