package phi

import java.nio.file.Path

import com.twitter.conversions.storage._
import com.twitter.util.StorageUnit

case class Config(
  baseDir: Path,
  maxSegmentSize: StorageUnit = 500 megabytes,
  logFlushIntervalMessages: Int = 1000,
  offsetFlushIntervalUpdates: Int = 1000
)
