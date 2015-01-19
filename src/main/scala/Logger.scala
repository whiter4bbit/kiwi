package phi

import com.twitter.logging.{Logger => TLogger}

trait Logger {
  val log = TLogger(getClass)
}
