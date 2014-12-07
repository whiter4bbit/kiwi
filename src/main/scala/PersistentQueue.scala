package phi

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.HashMap

class Producer(log: Log) {
  def append(payload: Array[Byte]) = 
    log.append(payload)
  def append(set: AppendMessageSet) = 
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
  private val logs = CachedResource((topic: String) => new Log(baseDir, topic))
  private val globalOffsets = CachedResource((topic: String) => new LogOffset(baseDir, topic))
  private val consumerOffsets = CachedResource((p: (String, String)) => new LogOffset(baseDir, p._1, Some(p._2)))

  def getConsumer(topic: String) =
    new GlobalConsumer(logs.get(topic), globalOffsets.get(topic))

  def getConsumer(topic: String, consumer: String) =
    new OffsetConsumer(logs.get(topic), consumerOffsets.get(topic, consumer))

  def getOffset(topic: String, consumer: String) =
    consumerOffsets.get(topic, consumer)

  def getProducer(topic: String) =
    new Producer(logs.get(topic))
}
