package phi.json

import org.jboss.netty.buffer.ChannelBuffer
import org.json4s._
import org.json4s.native.JsonMethods
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._

import phi.bytes._
import phi.message._

object JsonMessageFormat {
  implicit val formats = DefaultFormats

  private def fromJson(json: String): Result[List[Message]] = {
    try {
      val messages = JsonMethods.parse(json).extract[List[String]]
        .map(string => Message(string.getBytes))
      Success(messages)
    } catch {
      case e: Exception => Failure(Throw(e))
    }
  }

  def fromJson(buffer: ChannelBuffer, format: MessageBinaryFormat): Result[ByteChunk] = {
    for {
      messages <- fromJson(new String(buffer.array))
      chunk <- BinaryFormatWriter(format).toByteChunk(messages)
    } yield chunk
  }

  def toJson(chunk: ByteChunk, format: MessageBinaryFormat): String = {
    val messages = BinaryFormatReader(format).fromByteChunk(chunk)
    val strings = messages.map(message => new String(message.payload))
    JsonMethods.compact(JsonMethods.render(strings))
  }
}
