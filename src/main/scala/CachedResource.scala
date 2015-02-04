package phi

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConversions._

class CachedResource[K, R](create: K => R) {
  private val resources = new ConcurrentHashMap[K, R]

  def get(key: K): R = resources.get(key) match {
    case null => resources.synchronized {
      if (!resources.containsKey(key)) {
        val resource = create(key)
        resources.put(key, resource)
        resource
      } else resources.get(key)
    }
    case resource => resource
  }

  def getAll: Iterable[R] = resources.values
}

object CachedResource {
  def apply[K, R](create: K => R) = new CachedResource(create)
}
