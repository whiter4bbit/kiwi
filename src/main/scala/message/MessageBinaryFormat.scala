package phi.message

import scala.annotation.tailrec

import phi.bytes._

case class MessageBinaryFormat(maxMessageSize: Int) extends BinaryFormat[Message] {
  private def validateLength(length: Result[Int]): Result[Int] = length match {
    case s @ Success(length) if length <= maxMessageSize => s
    case Success(length) => Failure(Throw(MessageSizeExceedsLimit(limit = maxMessageSize, length)))
    case f: Failure => f
  }

  def read(reader: ByteChunkReader) = for {
    length <- validateLength(reader.readInt)
    payload <- reader.readBytes(Array.ofDim[Byte](length))
  } yield Message(payload)

  def write(messages: List[Message], builder: ByteChunkBuilder): Result[ByteChunk] = {
    @tailrec def write(messages: List[Message]): Result[Unit] = messages match {
      case message::tail => {
        val result = for {
          _ <- builder.writeInt(message.payload.length)
          _ <- builder.writeBytes(message.payload)
        } yield ()
        result match {
          case s: Success[_] => write(tail)
          case f: Failure => f
        }
      }
      case _ => Success(())
    }
    write(messages).map(_ => builder.build)
  }
}

