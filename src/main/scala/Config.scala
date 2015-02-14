package phi

import java.nio.file.{Path => JPath}
import java.io.{File => JFile}
import java.net.InetSocketAddress

import com.typesafe.config.{Config => HOCON, ConfigFactory => HOCONFactory}
import com.twitter.conversions.storage._
import com.twitter.util.StorageUnit

case class Config (
  baseDir: JPath,
  bindAddress: InetSocketAddress,
  maxSegmentSize: StorageUnit,
  logFlushIntervalMessages: Int,
  offsetFlushIntervalUpdates: Int
)

object Config {
  private def getInetSocketAddress(hocon: HOCON, path: String): InetSocketAddress = {
    val addr = hocon.getString(path)
    addr.split(":") match {
      case Array("", port) => new InetSocketAddress(port.toInt)
      case Array(host, port) => new InetSocketAddress(host, port.toInt)
      case _ => throw new IllegalStateException(s"Can't parse InetSocketAddress: '${addr}'")
    }
  }
  
  def apply(hocon: HOCON): Config = new Config(
    baseDir = new JFile(hocon.getString("kiwi.base-dir")).toPath,
    bindAddress = getInetSocketAddress(hocon, "kiwi.bind-address"),
    maxSegmentSize = new StorageUnit(hocon.getBytes("kiwi.max-segment-size")),
    logFlushIntervalMessages = hocon.getInt("kiwi.log-flush-interval-messages"),
    offsetFlushIntervalUpdates = hocon.getInt("kiwi.offset-flush-interval-updates")
  )

  def fromFile(file: JFile): Config = {
    apply(HOCONFactory.parseFile(file).withFallback(HOCONFactory.load()))
  }

  def fromFile(path: String): Config = {
    fromFile(new JFile(path))
  }

  def apply(): Config = {
    apply(HOCONFactory.load())
  }
}
