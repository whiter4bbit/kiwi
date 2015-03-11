package phi

import phi.bytes._

case class ByteChunkAndOffset(startOffset: Long, chunk: ByteChunk) {
  def offset = startOffset + chunk.length
}
