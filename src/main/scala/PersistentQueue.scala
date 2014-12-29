package phi

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.HashMap

import com.twitter.conversions.storage._
import com.twitter.util.StorageUnit

import phi.message.TransferableMessageSet

class Producer(log: Log) {
  def append(payload: Array[Byte]): Unit = 
    log.append(payload)
  def append(set: TransferableMessageSet): Unit = 
    log.append(set)
}

class CachedResource[K, R](create: K => R) {
  private val resources = HashMap.empty[K, R]

  def get(key: K): R = this.synchronized {
    resources.get(key) match {
      case Some(resource) => resource
      case None => {
        val resource = create(key)
        resources.put(key, resource)
        resource
      }
    }
  }
}

object CachedResource {
  def apply[K, R](create: K => R) = new CachedResource(create)
}

class PersistentQueue(baseDir: Path, maxSegmentSize: StorageUnit = 500 megabytes, 
    logFlushIntervalMessages: Int = 1000, offsetFlushIntervalUpdates: Int = 1000) {

  private val logs = CachedResource((topic: String) => Log.open(baseDir, topic, maxSegmentSize, logFlushIntervalMessages))
  private val offsetStorage = CachedResource((topic: String) => LogOffsetStorage.open(baseDir, topic, offsetFlushIntervalUpdates))
  private val producers = CachedResource((topic: String) => new Producer(logs.get(topic)))
  private val globalConsumers = CachedResource((topic: String) => new GlobalConsumer(logs.get(topic), offsetStorage.get(topic)))
  private val offsetConsumers = CachedResource { p: (String, String) =>
    val (topic, consumer) = p
    new OffsetConsumer(logs.get(topic), consumer, offsetStorage.get(topic))
  }

  def getConsumer(topic: String) =
    globalConsumers.get(topic)

  def getConsumer(topic: String, consumer: String) =
    offsetConsumers.get(topic, consumer)

  def getOffsetStorage(topic: String) =
    offsetStorage.get(topic)

  def getProducer(topic: String) =
    producers.get(topic)
}
