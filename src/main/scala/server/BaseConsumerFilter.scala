package phi.server

import scala.concurrent.duration._

import com.twitter.finagle.{SimpleFilter, Service}
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.path._
import com.twitter.finagle.http.Method._
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http._

import phi.{Logger, Consumer, Kiwi}

trait BaseConsumerFilter extends SimpleFilter[HttpRequest, HttpResponse] with Logger {
  private[server] val kiwi: Kiwi

  def next(consumer: Consumer, n: Int) = {
    Future.value(QueueHttpResponse.ok(consumer.next(n)))
  }

  def await(consumer: Consumer, max: Int, timeout: FiniteDuration = 5 seconds) = {
    val await = kiwi.getAwaitableConsumer().await(consumer, max, timeout)
    await.map(QueueHttpResponse.ok)
  }
}
