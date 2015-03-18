package phi.server

import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.{Status, Request, Response}
import com.twitter.finagle.http.Method._
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._

import phi.Kiwi
import phi.message._

class OffsetFilter(kiwi: Kiwi) extends SimpleFilter[Request, Response] {
 def apply(req: Request, service: Service[Request, Response]): Future[Response] = {
   (req.getMethod, Path(req.getUri)) match {
     case Post -> Root / topic / consumer / "offset" / Long(offset) if offset > 0 => {
       kiwi.getOffsetStorage(topic).put(consumer, offset)
       Future.value(Response())
     }

     case Get -> Root / topic / consumer / "offset" => {
       kiwi.getOffsetStorage(topic).get(consumer) match {
         case Some(offset) => Future.value(RichResponse(offset.toString))
         case None => Future.value(Response(Status.NotFound))
       }
     }
     case _ => service(req)
   }
 }
}
