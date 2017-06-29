/*
 * The MIT License
 *
 * Copyright (c) 2016-2017 Fulcrum Genomics LLC
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package dagr.webservice

import akka.actor.ActorSystem
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.HttpOriginRange
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{Route, _}
import akka.stream.ActorMaterializer
import ch.megard.akka.http.cors.scaladsl.settings.CorsSettings
import dagr.api.DagrApi
import upickle.Js
import upickle.Js.Value
import upickle.default.{Reader, Writer}

import scala.concurrent.{ExecutionContext, Future}


/** A simple trait to set CORS headers. Mix this in and wrap your routes with `corsFilter`. */
trait CorsDirectirves {
  import ch.megard.akka.http.cors.scaladsl.CorsDirectives._

  // FIXME: we don not want all origins!!!
  private  val corsSettings: CorsSettings = CorsSettings.defaultSettings.copy(allowedOrigins = HttpOriginRange.*)

  private val rejectionHandler: RejectionHandler = corsRejectionHandler withFallback RejectionHandler.default

  // Your exception handler
  private val exceptionHandler: ExceptionHandler = ExceptionHandler {
    case e: NoSuchElementException => complete(StatusCodes.NotFound -> e.getMessage)
  }

  // Combining the two handlers only for convenience
  private val handleErrors: Directive[Unit] = handleRejections(rejectionHandler) & handleExceptions(exceptionHandler)

  def corsFilter(route: Route): Route = {
    // NB: rejections and exceptions are handled *before* the CORS directive (in the inner route) so that the correct
    // CORS headers in the response even when an error occurs
    cors(corsSettings) {
      handleErrors {
        route
      }
    }
  }

}

object DagrApiService extends dagr.core.config.Configuration {
  val _version: String = optionallyConfigure[String](Configuration.Keys.WebServiceVersion).getOrElse("v1")
  val _root: String    =  optionallyConfigure[String](Configuration.Keys.WebServiceRoot).getOrElse("service")
}

/** Defines the possible routes for the Dagr service */
class DagrApiService(val taskInfoTracker: TaskInfoTracker)
                             (implicit val system: ActorSystem,
                              implicit val materializer: ActorMaterializer,
                              implicit val executionContext: ExecutionContext)
                              //implicit val executionContextExecutor: ExecutionContextExecutor)
    extends CorsDirectirves
    with DagrApiServerImpl
    with autowire.Server[Js.Value, upickle.default.Reader, upickle.default.Writer] {
  import DagrApiService._

  override def write[Result](r: Result)(implicit writer: Writer[Result]): Js.Value = {
    upickle.default.writeJs[Result](r)
  }

  override def read[Result](p: Value)(implicit reader: Reader[Result]): Result = {
    upickle.default.readJs[Result](p)
  }

  val routes: Route = corsFilter(versionRoute ~ queryRoute)

  private val prefixSegments = List("dagr", "api", "DagrApi")


  def versionRoute: Route = {
    path(_root / "version") {
      pathEnd {
        get {
          complete(_version)
        }
      }
    }
  }

  def queryRoute: Route = {
    pathPrefix(_root / _version / Segments) { segments =>
      post {
        entity(as[String]) { inputJson =>
          println(s"segments are: ${segments}")
          val request = autowire.Core.Request.apply(
            segments,
            upickle.json.read(inputJson).asInstanceOf[Js.Obj].value.toMap
          )
          println(s"request: $request")
          val result: Future[String] = this.route[DagrApi](this)(request).map(upickle.default.write(_))
          onSuccess(result) { str: String => complete(str) }
        }
      }
    }
  }
}
