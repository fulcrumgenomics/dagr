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

import akka.actor.{Actor, ActorSystem, Props}
import akka.util.Timeout
import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.DagrDef._
import dagr.core.tasksystem.Task.TaskInfoLike
import dagr.webservice.PerRequest.RequestComplete
import spray.http.StatusCodes
import spray.httpx.SprayJsonSupport._

import scala.concurrent.duration._

/** This object stores the definitions for the API requests. */
object DagrApiHandler {
  def props(taskInfoTracker: TaskInfoTracker): Props = {
    Props(new DagrApiHandler(taskInfoTracker))
  }

  sealed trait DagrRequest
  final case class DagrVersionRequest() extends DagrRequest
  final case class DagrStatusRequest() extends DagrRequest
  final case class DagrTaskScriptRequest(id: TaskId) extends DagrRequest
  final case class DagrTaskLogRequest(id: TaskId) extends DagrRequest
  final case class DagrTaskInfoRequest(id: TaskId) extends DagrRequest
}

/** Receives a request, performs the appropriate logic, and sends back a response */
class DagrApiHandler(val taskInfoTracker: TaskInfoTracker) extends Actor with DagrApiJsonSupport with LazyLogging {
  // needed for marshalling
  import DagrApiHandler._

  implicit val timeout: Timeout    = Timeout(2.seconds)
  implicit val system: ActorSystem = context.system

  override def receive: PartialFunction[Any, Unit] = {
    case DagrVersionRequest()      =>
      context.parent ! RequestComplete(StatusCodes.OK, DagrVersionResponse(DagrApiService.version))
    case DagrStatusRequest()       =>
      context.parent ! RequestComplete(StatusCodes.OK, DagrStatusResponse(taskInfoTracker.infos))
    case DagrTaskScriptRequest(id) =>
      applyTaskInfo(id, info => context.parent ! RequestComplete(StatusCodes.OK, DagrTaskScriptResponse(info.script)))
    case DagrTaskLogRequest(id)    =>
      applyTaskInfo(id, info => context.parent ! RequestComplete(StatusCodes.OK, DagrTaskLogResponse(info.log)))
    case DagrTaskInfoRequest(id)   =>
      applyTaskInfo(id, info => context.parent ! RequestComplete(StatusCodes.OK, DagrTaskInfoResponse(info)))
  }

  /** Handles the case that the specific task identifier does not exist and sends back a bad request message, otherwise,
    * it applies the given method.
    */
  private def applyTaskInfo(id: TaskId, f: (TaskInfoLike => Unit)): Unit = {
    taskInfoTracker.info(id) match {
      case Some(info) => f(info)
      case None => context.parent ! RequestComplete(StatusCodes.NotFound, s"Task with id '$id' not found")
    }
  }
}

