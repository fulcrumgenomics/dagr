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

package dagr.server

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.api.DagrApiConfiguration
import dagr.core.DagrDef.TaskId
import dagr.core.config.Configuration
import dagr.core.exec.Executor
import dagr.core.reporting.ReportingDef.{TaskLogger, TaskRegister}
import dagr.core.tasksystem.Task
import dagr.api.models.TaskStatus
import dagr.core.tasksystem.Task.TaskInfoLike

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object TaskInfoTracker {
  case class Info(info: TaskInfoLike, parent: TaskInfoLike, children: Seq[dagr.api.models.TaskInfo])
}

/** A little class to store all the infos created by the execution system. */
class TaskInfoTracker(executor: Executor) extends TaskLogger with TaskRegister {
  import DagrApiConversions.toTaskInfo

  private val _infos      = mutable.TreeSet[TaskInfoLike]()
  private val _idToInfo   = mutable.HashMap[TaskId,TaskInfoLike]()
  private val _toParent   = mutable.HashMap[Task,Task]() // child -> parent
  private val _toChildren = mutable.HashMap[Task,Seq[Task]]()
  private val _report      = ListBuffer[String]()

  val statuses: Seq[TaskStatus] = executor.statuses

  /** Returns the task status by ordinal */
  def statusFrom(ordinal: Int): TaskStatus = executor.statusFrom(ordinal)

  def record(info: TaskInfoLike): Unit = {
    _report += info.infoString
    // Add the info (only once)
    if (!_infos.contains(info)) _infos += info
    // Add an entry only when the id is defined
    info.id.foreach { id =>
      if (!this._idToInfo.contains(id)) this._idToInfo(id) = info
    }
  }

  def register(parent: Task, child: Task*): Unit = {
    child.foreach { c =>
      if (!_toParent.contains(c)) _toParent(c) = parent
    }
    if (child.isEmpty) _toParent(parent) = parent
    _toChildren(parent) = child.filterNot(_ == parent)
  }

  def report: Seq[String] = this._report.toList

  def info(id: TaskId): Option[TaskInfoLike] = this._idToInfo.get(id)

  def infos: Iterable[TaskInfoLike] = this._infos

  // FIXME: rename
  def fullInfo: Iterable[TaskInfoTracker.Info] = {
    this._infos.map { info: TaskInfoLike =>
      val children: Seq[dagr.api.models.TaskInfo] = _toChildren.get(info.task) match {
        case Some(_children) => _children.map { child: Task =>
          child._taskInfo.map(i => toTaskInfo(i)).getOrElse {
            new dagr.api.models.TaskInfo(
              name           = child.name,
              id             = None,
              attempts       = -1,
              script         = None,
              log            = None,
              resources      = None,
              exitCode       = None,
              throwable      = None,
              status         = null, // FIXME
              timePoints     = Seq.empty,
              statusTime     = null, // FIXME,
              scriptContents = None,
              logContents    = None
            )
          }
        }
        case None => Seq.empty
      }

      TaskInfoTracker.Info(
        info     = info,
        parent   = _toParent(info.task).taskInfo,
        children = children
      )
    }
  }
}

object DagrServer extends Configuration {
  object Keys {
    val WebServiceHost: String = "dagr.webservice.host"
    val WebServicePort: String = "dagr.webservice.port"
  }
}


/** Dagr web service */
class DagrServer(executor: Executor, host: Option[String], port: Option[Int])(implicit ex: ExecutionContext)
  extends DagrApiConfiguration with LazyLogging {

  implicit val actorSystem: ActorSystem        = ActorSystem("dagr-server")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  private val taskInfoTracker = new TaskInfoTracker(executor)

  private val _host = host.getOrElse("0.0.0.0")

  private val _port = port.getOrElse(8080)

  private val service = new DagrApiService(taskInfoTracker)

  /** The logger to track all the task infos. */
  def taskLogger: TaskLogger = this.taskInfoTracker

  def startAllServices() {
    startWebServiceActors()
  }

  def stopAllServices() {
    stopAndCatchExceptions(stopWebServiceActors())
  }

  private def startWebServiceActors() = {
    implicit val bindTimeout: Timeout = 120.seconds
    Await.ready(Http().bindAndHandle(service.routes, interface=this._host, port=this._port), bindTimeout.duration) onComplete {
      case Success(_) =>
        logger.info("Actor system started.")
      case Failure(_) =>
        logger.error(s"Unable to bind to port ${this._port} on interface ${this._host}")
        Await.result(actorSystem.terminate(), 60.seconds)
        stopAndExit()
    }
  }

  private def stopWebServiceActors(): Unit = {
    Await.result(Http().shutdownAllConnectionPools(), 120.seconds)
  }

  private def stopAndExit() {
    logger.info("Stopping all services and exiting.")
    stopAllServices()
    logger.info("Services stopped")
    throw new RuntimeException("Errors were found while initializing Dagr.  This server will shutdown.")
  }

  private def stopAndCatchExceptions(closure: => Unit) {
    Try(closure).recover {
      case ex: Throwable => logger.error("Exception ignored while shutting down.", ex)
    }
  }
}
