/*
 * The MIT License
 *
 * Copyright (c) 2017 Fulcrum Genomics LLC
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
 *
 */

package dagr.webservice

import java.nio.file.Path

import com.fulcrumgenomics.commons.CommonsDef.FilePath
import com.fulcrumgenomics.commons.util.LogLevel
import com.fulcrumgenomics.sopt.arg
import dagr.core.cmdline.{DagrCoreArgs, DagrCoreMain}
import dagr.core.exec._
import dagr.core.tasksystem.Pipeline

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object DagrServerMain extends dagr.core.config.Configuration {
  /** The packages we wish to include in our command line **/
  protected def getPackageList: List[String] = {
    val config = new dagr.core.config.Configuration {}
    config.optionallyConfigure[List[String]](dagr.core.config.Configuration.Keys.PackageList) getOrElse List[String]("dagr")
  }

  /** The main method */
  def main(args: Array[String]): Unit = {
    new DagrServerMain[DagrServerArgs]().makeItSoAndExit(args)
  }
}

class DagrServerMain[T<:DagrCoreArgs:TypeTag:ClassTag] extends DagrCoreMain[T]

class DagrServerArgs(
                    // TODO: update args with precedence information
                    @arg(doc = "Load in a custom configuration into Dagr.  See https://github.com/typesafehub/config for details on the file format.")
                    override val config: Option[Path] = None,
                    @arg(doc = "Stop pipelines immediately on detecting the first failed task.")
                    override val failFast: Boolean = false,
                    @arg(doc = "Overrides the default scripts directory in the configuration file.")
                    override val scriptDir: Option[Path] = None,
                    @arg(doc = "Overrides default log directory in the configuration file.")
                    override val logDir: Option[Path] = None,
                    @arg(doc = "Set the logging level.", flag='l')
                    override val logLevel: LogLevel = LogLevel.Info,
                    @arg(doc = "Dagr scala scripts to compile and add to the list of programs (must end with .dagr or .scala).", minElements=0)
                    override val scripts: List[Path] = Nil,
                    @arg(doc = "Set the number of cores available to dagr.")
                    override val cores: Option[Double] = None,
                    @arg(doc = "Set the memory available to dagr.")
                    override val memory: Option[String] = None,
                    @arg(doc = "Write an execution report to this file, otherwise write to the stdout")
                    override val report: Option[Path] = None,
                    @arg(doc = "Provide an top-like interface for tasks with the give delay in seconds. This suppress info logging.")
                    interactive: Boolean = false,
                    @arg(doc = "Use the experimental execution system.")
                    override val experimentalExecution: Boolean = false,
                    @arg(doc = "Attempt to replay using the provided replay log")
                    override val replayLog: Option[FilePath] = None,
                    @arg(doc = "Run the Dagr web-service. Dagr will not exit once all tasks have completed.")
                    val webservice: Boolean = false,
                    @arg(doc = "The host name for the web-service.")
                    val host: Option[String] = None,
                    @arg(doc = "The port for teh web-service.")
                    val port: Option[Int] = None
                  ) extends DagrCoreArgs(config, failFast, scriptDir, logDir, logLevel, scripts, cores, memory, report, interactive, experimentalExecution, replayLog) {

  override protected[dagr] def configure(pipeline: Pipeline, commandLine: Option[String])(implicit ex: ExecutionContext): Unit = {
    super.configure(pipeline, commandLine)
  }

  override protected def executeSetup(executor: Executor, report: FilePath)(implicit ex: ExecutionContext): Unit = {
    super.executeSetup(executor, report)

    // Set up the web service
    if (webservice) {
      val server = new DagrServer(executor, this.host, this.port)
      server.startAllServices()
      sys addShutdownHook server.stopAllServices()
      executor.withReporter(server.taskLogger)
    }
  }

  override protected def executeFinish(executor: Executor, report: FilePath): Unit = {
    super.executeFinish(executor, report)
    if (webservice) {
       while (true) Thread.sleep(500)
    }
  }
}
