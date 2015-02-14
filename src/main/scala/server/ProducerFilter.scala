package phi.server

import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.Method._
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http._

import phi.Kiwi
import phi.message._

class ProducerFilter(kiwi: Kiwi) extends SimpleFilter[HttpRequest, HttpResponse] {
 def apply(req: HttpRequest, service: Service[HttpRequest, HttpResponse]): Future[HttpResponse] = {
   (req.getMethod, Path(req.getUri)) match {
     case Post -> Root / topic => {
       val batch = ChannelBufferMessageBatch(req.getContent)
       kiwi.getProducer(topic).append(batch)
       Future.value(Response())
     }
     case _ => service(req)
   }
 }
}