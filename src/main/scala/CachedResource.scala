package phi

import scala.collection.mutable.HashMap

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

  def getAll: Iterable[R] = resources.values
}

object CachedResource {
  def apply[K, R](create: K => R) = new CachedResource(create)
}

