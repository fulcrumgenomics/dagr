/*
 * The MIT License
 *
 * Copyright (c) 2016 Fulcrum Genomics LLC
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

import akka.actor.{Actor, ActorContext, Props}
import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.DagrDef.TaskId

import spray.http.HttpHeaders.RawHeader
import spray.http._
import spray.http.StatusCodes._
import spray.httpx.marshalling.Marshaller
import spray.routing._
import spray.routing.directives.RouteDirectives
import spray.util.LoggingContext
import spray.json._
import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success, Try}

object DagrApiServiceActor {
  def props(taskInfoTracker: TaskInfoTracker): Props = Props(classOf[DagrApiServiceActor], taskInfoTracker)
}

/** The main actor for the API service */
class DagrApiServiceActor(taskInfoTracker: TaskInfoTracker) extends HttpServiceActor with LazyLogging {
  implicit val executionContext: ExecutionContextExecutor = actorRefFactory.dispatcher

  trait ActorRefFactoryContext {
    def actorRefFactory: ActorContext = context
  }
  override def actorRefFactory: ActorContext = context

  val dagrService: DagrApiService = new DagrApiService(taskInfoTracker) with ActorRefFactoryContext
  val possibleRoutes: Route = options{ complete(OK) } ~ dagrService.routes

  def receive: Actor.Receive = runRoute(possibleRoutes)

  implicit def routeExceptionHandler(implicit log: LoggingContext) =
    ExceptionHandler {
      case e: IllegalArgumentException => complete(BadRequest, e.getMessage)
      case ex: Throwable =>
        logger.error(ex.getMessage)
        complete(InternalServerError, s"Something went wrong, but the error is unspecified.")
    }

  implicit val routeRejectionHandler =
    RejectionHandler {
      case MalformedRequestContentRejection(message, cause) :: _ => complete(BadRequest, message)
    }
}

/** A simple trait to set CORS headers. Mix this in and wrap your routes with `respondWithCORSHeaders`. */
trait CORSDirectives {
  this: HttpService =>

  def respondWithCORSHeaders(origin: AllowedOrigins): Directive0 = respondWithHeaders(HttpHeaders.`Access-Control-Allow-Origin`(origin))

  def corsFilter(origin: String)(route: Route): Route = {
    if (origin == "*") {
      respondWithCORSHeaders(AllOrigins)(route)
    }
    else {
      optionalHeaderValueByName("Origin") {
        case None => route
        case Some(clientOrigin) if origin == clientOrigin =>
          respondWithCORSHeaders(SomeOrigins(Seq(HttpOrigin(origin))))(route)
        case Some(_) =>
          complete(Forbidden, Nil, "Invalid origin") // Maybe, a Rejection will fit better
      }
    }
  }
}

object DagrApiService {
  val version: String = "v1" // TODO: match versions
  val root: String    = "service"
}

/** Defines the possible routes for the Dagr service */
abstract class DagrApiService(val taskInfoTracker: TaskInfoTracker) extends HttpService with PerRequestCreator with DagrApiJsonSupport with CORSDirectives {
  import DagrApiService._

  // FIXME: we don not want all origins!!!
  def routes: Route = corsFilter("*") { versionRoute ~ taskScriptRoute ~ taskLogRoute ~ infoRoute }

  def versionRoute: Route = {
    path(root / "version") {
      pathEnd {
        get {
          requestContext => perRequest(requestContext, DagrApiHandler.props(taskInfoTracker), DagrApiHandler.DagrVersionRequest())
        }
      }
    }
  }

  def taskScriptRoute: Route = {
    path(root / version / "script" / IntNumber) { (id) =>
      get {
        Try(TaskId(id)) match {
          case Success(taskId) =>
            requestContext => perRequest(requestContext, DagrApiHandler.props(taskInfoTracker), DagrApiHandler.DagrTaskScriptRequest(taskId))
          case Failure(ex) => complete(StatusCodes.BadRequest)
        }
      }
    }
  }

  def taskLogRoute: Route = {
    path(root / version / "log" / IntNumber) { (id) =>
      get {
        Try(TaskId(id)) match {
          case Success(taskId) =>
            requestContext => perRequest(requestContext, DagrApiHandler.props(taskInfoTracker), DagrApiHandler.DagrTaskLogRequest(taskId))
          case Failure(ex) => complete(StatusCodes.BadRequest)
        }
      }
    }
  }

  def infoRoute: Route = {
    pathPrefix(root / version / "info") {
      pathEnd {
        get {
          requestContext => perRequest(requestContext, DagrApiHandler.props(taskInfoTracker), DagrApiHandler.DagrTaskInfosRequest())
        }
      } ~
      pathEnd {
        post {
          entity(as[TaskInfoQuery]) { query =>
            requestContext => perRequest(requestContext, DagrApiHandler.props(taskInfoTracker), DagrApiHandler.DagrTaskInfoQueryRequest(query))
          }
        }
      } ~
      pathPrefix(IntNumber) { (id) =>
        get {
          Try(TaskId(id)) match {
            case Success(taskId) =>
              requestContext => perRequest(requestContext, DagrApiHandler.props(taskInfoTracker), DagrApiHandler.DagrTaskInfoRequest(taskId))
            case Failure(ex) => complete(StatusCodes.BadRequest)
          }
        }
      }

    }
  }

  def rawJson = extract { _.request.entity.asString}
}