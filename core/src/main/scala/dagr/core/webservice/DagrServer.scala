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

import akka.io.IO
import akka.io.Tcp.CommandFailed
import akka.util.Timeout
import akka.actor.ActorSystem
import akka.pattern._
import dagr.commons.util.LazyLogging
import dagr.core.config.Configuration
import dagr.core.execsystem.TaskManagerLike
import spray.can.Http
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/** Configuration for the dagr server */
object DagrServer extends Configuration {
  lazy val host = optionallyConfigure[String](Configuration.Keys.WebServiceHost).getOrElse("0.0.0.0")
  lazy val port = optionallyConfigure[String](Configuration.Keys.WebSErvicePort).getOrElse("80").toInt
}

/** Dagr web service */
class DagrServer(val taskManager: TaskManagerLike) extends LazyLogging {
  implicit val actorSystem = ActorSystem("dagr")

  DagrApiJsonSupport.taskManager = taskManager

  def startAllServices() {
    startWebServiceActors()
  }

  def stopAllServices() {
    stopAndCatchExceptions(stopWebServiceActors())
  }

  private def startWebServiceActors() = {
    implicit val bindTimeout: Timeout = 120.seconds
    val service = actorSystem.actorOf(DagrApiServiceActor.props(taskManager), "dagr-actor")
    Await.result(IO(Http) ? Http.Bind(service, interface = DagrServer.host, port = DagrServer.port), bindTimeout.duration) match {
      case CommandFailed(b: Http.Bind) =>
        logger.error(s"Unable to bind to port ${DagrServer.port} on interface ${DagrServer.host}")
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
