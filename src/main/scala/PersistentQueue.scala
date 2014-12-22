package phi

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.HashMap

import phi.message.TransferableMessageSet

class Producer(log: Log) {
  def append(payload: Array[Byte]) = 
    log.append(payload)
  def append(set: TransferableMessageSet) = 
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

class PersistentQueue(baseDir: Path) {
  private val logs = CachedResource((topic: String) => Log.open(baseDir, topic))
  private val globalOffsets = CachedResource((topic: String) => new LogOffset(baseDir, topic))
  private val consumerOffsets = CachedResource((p: (String, String)) => new LogOffset(baseDir, p._1, Some(p._2)))
  private val producers = CachedResource((topic: String) => new Producer(logs.get(topic)))
  private val globalConsumers = CachedResource((topic: String) => new GlobalConsumer(logs.get(topic), globalOffsets.get(topic)))
  private val offsetConsumers = CachedResource { p: (String, String) =>
    val (topic, consumer) = p
    new OffsetConsumer(logs.get(topic), consumerOffsets.get(topic, consumer))
  }

  def getConsumer(topic: String) =
    globalConsumers.get(topic)

  def getConsumer(topic: String, consumer: String) =
    offsetConsumers.get(topic, consumer)

  def getOffset(topic: String, consumer: String) =
    consumerOffsets.get(topic, consumer)

  def getProducer(topic: String) =
    producers.get(topic)
}
