package phi.server

object Exceptions {
  case class UnsupportedContentType(contentType: String) extends Exception
}
