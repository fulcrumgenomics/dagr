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
import akka.io.IO
import akka.io.Tcp.CommandFailed
import akka.pattern._
import akka.util.Timeout
import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.DagrDef.TaskId
import dagr.core.exec.Executor
import dagr.core.reporting.ReportingDef.TaskLogger
import dagr.core.tasksystem.Task.{TaskInfoLike, TaskStatus}
import spray.can.Http

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try


/** A little class to store all the infos created by the execution system. */
class TaskInfoTracker(executor: Executor) extends TaskLogger {
  private val _infos = mutable.TreeSet[TaskInfoLike]()
  private val _idToInfo = mutable.HashMap[TaskId,TaskInfoLike]()

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

  def info(id: TaskId): Option[TaskInfoLike] = this._idToInfo.get(id)

  def infos: Iterable[TaskInfoLike] = this._infos

  // TODO: different methods of getting the info...
}

/** Dagr web service */
class DagrServer(executor: Executor, host: Option[String], port: Option[Int]) extends dagr.core.config.Configuration with LazyLogging {
  implicit val actorSystem = ActorSystem("dagr")

  private val taskInfoTracker = new TaskInfoTracker(executor)

  private val _host = host.getOrElse {
    optionallyConfigure[String](Configuration.Keys.WebServiceHost).getOrElse("0.0.0.0")
  }

  private val _port = port.getOrElse {
    optionallyConfigure[String](Configuration.Keys.WebSErvicePort).getOrElse("8080").toInt
  }

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
    val service = actorSystem.actorOf(DagrApiServiceActor.props(taskInfoTracker), "dagr-actor")
    Await.result(IO(Http) ? Http.Bind(service, interface = this._host, port = this._port), bindTimeout.duration) match {
      case CommandFailed(b: Http.Bind) =>
        logger.error(s"Unable to bind to port ${this._port} on interface ${this._host}")
        actorSystem.shutdown()
        stopAndExit()
      case _ => logger.info("Actor system started.")
    }
  }

  private def stopWebServiceActors() {
    IO(Http) ! Http.CloseAll
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
