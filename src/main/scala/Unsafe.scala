package phi

object Exceptions {
  def swallow[A](f: => A): Unit = {
    try f catch {
      case e: Exception => //pass
    } 
  }
}
