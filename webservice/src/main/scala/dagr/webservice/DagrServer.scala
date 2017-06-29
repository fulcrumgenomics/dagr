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

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.DagrDef.TaskId
import dagr.core.exec.Executor
import dagr.core.reporting.ReportingDef.{TaskLogger, TaskRegister}
import dagr.core.tasksystem.Task
import dagr.core.tasksystem.Task.{TaskInfoLike, TaskStatus}

import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object TaskInfoTracker {
  case class Info(info: TaskInfoLike, parent: TaskInfoLike, children: Seq[TaskInfoLike])
}

/** A little class to store all the infos created by the execution system. */
class TaskInfoTracker(executor: Executor) extends TaskLogger with TaskRegister {
  private val _infos = mutable.TreeSet[TaskInfoLike]()
  private val _idToInfo = mutable.HashMap[TaskId,TaskInfoLike]()
  private val _toParent = mutable.HashMap[Task,Task]() // child -> parent
  private val _toChildren = mutable.HashMap[Task,Seq[Task]]()

  /** Returns the task status by ordinal */
  def statusFrom(ordinal: Int): TaskStatus = executor.statusFrom(ordinal)

  def record(info: TaskInfoLike): Unit = {
    // Add the info (only once)
    if (!_infos.contains(info)) _infos += info
    // Add an entry only when the id is defined
    info.id.foreach { id =>
      if (!this._idToInfo.contains(id)) this._idToInfo(id) = info
    }
  }

  def register(parent: Task, child: Task*): Unit = {
    if (!child.forall(_ == parent)) {
      child.foreach {
        c => _toParent(c) = parent
      }
      _toChildren(parent) = child.toSeq
    }
  }

  def info(id: TaskId): Option[TaskInfoLike] = this._idToInfo.get(id)

  def infos: Iterable[TaskInfoLike] = this._infos

  // FIXME: rename
  def fullInfo: Iterable[TaskInfoTracker.Info] = {
    this._infos.map { info =>
      _toChildren(info.task).map { child =>
        child._taskInfo.getOrElse {
          import dagr.api.models.TaskInfo
          new TaskInfo(
            task       = child,
            name       = child.name,
            id         = None,
            attempts   = -1,
            script     = None,
            log        = None,
            resources  = None,
            exitCode   = None,
            throwable  = None,
            status     = null, // FIXME
            timePoints = Seq.empty,
            statusTime = null // FIXME
          )
        }
        null
      }

      TaskInfoTracker.Info(
        info     = info,
        parent   = _toParent(info.task).taskInfo,
        children = _toChildren(info.task).map(_.taskInfo) // _.taskInfo may not be defined yet!!!
      )
    }
  }
}

/** Dagr web service */
class DagrServer(executor: Executor, host: Option[String], port: Option[Int])(implicit ex: ExecutionContext) extends dagr.core.config.Configuration with LazyLogging {
  implicit val actorSystem: ActorSystem        = ActorSystem("dagr-server")
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  private val taskInfoTracker = new TaskInfoTracker(executor)

  private val _host = host.getOrElse {
    optionallyConfigure[String](Configuration.Keys.WebServiceHost).getOrElse("0.0.0.0")
  }

  private val _port = port.getOrElse {
    optionallyConfigure[String](Configuration.Keys.WebServicePort).getOrElse("8080").toInt
  }

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
