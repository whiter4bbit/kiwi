package phi

import com.twitter.logging.{Logger => TLogger}
import org.slf4j.LoggerFactory

trait Logger {
  val log = LoggerFactory.getLogger(getClass)
}
