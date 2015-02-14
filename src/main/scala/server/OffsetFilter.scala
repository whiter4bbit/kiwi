package phi.server

import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.Method._
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http._

import phi.Kiwi
import phi.message._

class OffsetFilter(kiwi: Kiwi) extends SimpleFilter[HttpRequest, HttpResponse] {
 def apply(req: HttpRequest, service: Service[HttpRequest, HttpResponse]): Future[HttpResponse] = {
   (req.getMethod, Path(req.getUri)) match {
     case Post -> Root / topic / consumer / "offset" / Long(offset) if offset > 0 => {
       kiwi.getOffsetStorage(topic).put(consumer, offset)
       Future.value(Response())
     }

     case Get -> Root / topic / consumer / "offset" => {
       kiwi.getOffsetStorage(topic).get(consumer) match {
         case Some(offset) => {
           val response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK)
           response.setContent(ChannelBuffers.wrappedBuffer(offset.toString.getBytes))
           Future.value(response)
         }
         case None => {
           Future.value(Response(HttpResponseStatus.NOT_FOUND))
         }
       }
     }
     case _ => service(req)
   }
 }
}
