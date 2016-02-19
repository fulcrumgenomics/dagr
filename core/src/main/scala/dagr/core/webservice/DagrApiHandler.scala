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

package dagr.core.webservice

import akka.actor.{Actor, Props}
import dagr.commons.util.LazyLogging
import dagr.core.execsystem.{TaskManagerLike, TaskExecutionInfo}
import dagr.core.webservice.PerRequest.RequestComplete
import spray.http.StatusCodes
import scala.concurrent.duration._
import akka.util.Timeout
import java.nio.file.Path
import spray.httpx.SprayJsonSupport._
import dagr.core.DagrDef._

/** This object stores the definitions for the API requests. */
object DagrApiHandler {
  def props(taskManager: TaskManagerLike): Props = {
    Props(new DagrApiHandler(taskManager))
  }

  sealed trait DagrRequest
  final case class DagrVersionRequest() extends DagrRequest
  final case class DagrStatusRequest() extends DagrRequest
  final case class DagrTaskScriptRequest(id: TaskId) extends DagrRequest
  final case class DagrTaskLogRequest(id: TaskId) extends DagrRequest
  final case class DagrTaskInfoRequest(id: TaskId) extends DagrRequest
}

/** Receives a request, performs the appropriate logic, and sends back a response */
class DagrApiHandler(val taskManager: TaskManagerLike) extends Actor with LazyLogging {
  // needed for marshalling
  import DagrApiJsonSupport._
  import context.dispatcher
  import DagrApiHandler._

  implicit val timeout = Timeout(2.seconds)
  implicit val system = context.system

  override def receive = {
    case DagrVersionRequest() =>
      context.parent ! RequestComplete(StatusCodes.OK, DagrVersionResponse(DagrApiService.version))
    case DagrStatusRequest() =>
      context.parent ! RequestComplete(StatusCodes.OK, DagrStatusResponse(taskManager.taskToInfoBiMap.values))
    case DagrTaskScriptRequest(id) =>
      applyTaskInfo(id, info => context.parent ! RequestComplete(StatusCodes.OK, DagrTaskScriptResponse(pathToString(info.script))))
    case DagrTaskLogRequest(id) =>
      applyTaskInfo(id, info => context.parent ! RequestComplete(StatusCodes.OK, DagrTaskLogResponse(pathToString(info.logFile))))
    case DagrTaskInfoRequest(id) =>
      applyTaskInfo(id, info => context.parent ! RequestComplete(StatusCodes.OK, DagrTaskInfoResponse(info)))
  }

  private def pathToString(path: Path): String = scala.io.Source.fromFile(path.toFile).mkString

  /** Handles the case that the specific task identifier does not exist and sends back a bad request message, otherwise,
    * it applies the given method.
    */
  private def applyTaskInfo(id: TaskId, f: (TaskExecutionInfo => Unit)): Unit = {
    taskManager.taskExecutionInfoFor(id) match {
      case Some(info) => f(info)
      case None => context.parent ! RequestComplete(StatusCodes.NotFound, s"Task with id '$id' not found")
    }
  }
}

