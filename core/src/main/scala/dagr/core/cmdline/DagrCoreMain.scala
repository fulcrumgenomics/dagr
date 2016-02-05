/*
 * The MIT License
 *
 * Copyright (c) 2015 Fulcrum Genomics LLC
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
package dagr.core.cmdline

import java.io.PrintWriter
import java.nio.file.{Files, Path}

import dagr.core.cmdline.parsing.DagrCommandLineParser
import dagr.core.config.Configuration
import dagr.core.execsystem._
import dagr.core.tasksystem.{Pipeline, ValidationException, Task}
import dagr.core.util.{LazyLogging, BiMap, LogLevel, Logger, Io}

import scala.collection.mutable.ListBuffer

object DagrCoreMain {
  /** The packages we wish to include in our command line **/
  protected def getPackageList: List[String] = {
    val config = new Configuration {}
    config.optionallyConfigure[List[String]](Configuration.Keys.PackageList) getOrElse List[String]("dagr")
  }

  /** The main method */
  def main(args: Array[String]): Unit = {
    makeItSo(args, packageList = getPackageList)
  }

  def makeItSo(args: Array[String], packageList: List[String] = getPackageList): Unit = {
    val parser: DagrCommandLineParser = new DagrCommandLineParser(Configuration.commandLineName)
    parser.parse(args, getPackageList) match {
      case Some((clp, pipeline)) => System.exit(clp.execute(pipeline))
      case None => System.exit(1)
    }
  }

  /** Provide an command line validation error message */
  private[cmdline] def buildErrorMessage(msgOption: Option[String] = None, exceptionOption: Option[Exception] = None): String = {
    val extraMsg: String = if (msgOption.isDefined) s"\nmessage: ${msgOption.get}" else ""
    exceptionOption match {
      case Some(e) =>
        s"${e.getClass.getCanonicalName}: ${Configuration.commandLineName} command line validation error: ${e.getMessage}$extraMsg"
      case None =>
        s"${Configuration.commandLineName} command line validation error$extraMsg"
    }
  }
}

/** The main class for Dagr.  Command line arguments are parsed here, and the given pipeline or task
  * is subsequently executed.
  *
  * @param config the path to an configuration file for any tasks-specific options.  See [[Configuration]] to
  *               access the parsed configuration.  The file may be a Java properties or JSON file.  See
  *               https://github.com/typesafehub/config for more information.
  * @param scriptDir the path to where scripts will be stored, holding the commands for
  *                   [[dagr.core.tasksystem.ProcessTask]] tasks.  If none is given, a temporary directory will
  *                   be created.
  * @param logDir the path to where log files will be stored, holding the commands for
  *               [[dagr.core.tasksystem.ProcessTask]] tasks.  If none is given, a temporary directory will
  *               be created.
  * @param logLevel the verbosity level for logging.
  * @param scripts a list of Dagr (scala) scripts to be included in the command line.  Use this option to inject and
  *                execute custom tasks and pipelines without having to re-compile Dagr.
  */
class DagrCoreMain(
// TODO: update args with precedence information
  @Arg(doc = "Load in a custom configuration into Dagr.  See https://github.com/typesafehub/config for details on the file format.", common = true)
  val config: Option[Path] = None,
  @Arg(doc = "Overrides the default scripts directory in the configuration file.", common = true)
  val scriptDir: Option[Path] = None,
  @Arg(doc = "Overrides default log directory in the configuration file.", common = true)
  val logDir: Option[Path] = None,
  @Arg(doc = "Set the logging level.", common = true, flag="l")
  val logLevel: LogLevel = LogLevel.Info,
  @Arg(doc = "Dagr scala scripts to compile and add to the list of programs.", common = true, minElements=0)
  val scripts: List[Path] = Nil,
  @Arg(doc = "Set the number of cores available to dagr.", common = true)
  val cores: Option[Double] = None,
  @Arg(doc = "Set the memory available to dagr.", common = true)
  val memory: Option[String] = None,
  @Arg(doc = "Write an execution report to this file, otherwise write to the stdout", common = true)
  val report: Option[Path] = None
) extends LazyLogging {

  // These are not optional, but are only populated during configure()
  private var taskManager : Option[TaskManager] = None
  private var reportPath  : Option[Path] = None

  // Initialize the configuration as early as possible
  Configuration.initialize(this.config)

  /** Takes a series of things that return Option[T] and returns the first defined one or None if none are defined. */
  private def pick[T](things : Option[T]*) : Option[T] = {
    things.find(_.isDefined) getOrElse None
  }

  /** Try to create a given directory, and if there is an exception, write the path since some exceptions can be obtuse */
  private def mkdir(dir: Path, use: String, errors: ListBuffer[String]): Unit = {
    try { Files.createDirectories(dir) }
    catch { case e: Exception => errors += DagrCoreMain.buildErrorMessage(Some(s"Could not create the $use directory: $dir")) }
  }

  // Invoked by DagrCommandLineParser after the pipeline has also been instantiated
  private[cmdline] def configure(pipeline: Pipeline) : Unit = {
    try {
      val config = new Configuration { }

      // System and JVM resources
      val systemCores  = config.optionallyConfigure[Double](Configuration.Keys.SystemCores) orElse this.cores
      val systemMemory = config.optionallyConfigure[String](Configuration.Keys.SystemMemory) orElse this.memory

      // scripts & logs directories
      val scriptsDirectory = pick(this.scriptDir, pipeline.outputDirectory.map(_.resolve("scripts")), config.optionallyConfigure(Configuration.Keys.ScriptDirectory))
      val logDirectory     = pick(this.logDir,    pipeline.outputDirectory.map(_.resolve("logs")),    config.optionallyConfigure(Configuration.Keys.LogDirectory))

      {
        val errors: ListBuffer[String] = ListBuffer[String]()
        scriptsDirectory.foreach(d => mkdir(dir=d, use="scripts", errors=errors))
        logDirectory.foreach(d => mkdir(dir=d, use="logs", errors=errors))
        if (errors.nonEmpty) throw new ValidationException(errors.toList)
      }

      // report file
      this.reportPath = pick(report, pipeline.outputDirectory.map(_.resolve("execution_report.txt")), Option(Io.StdOut)).map(_.toAbsolutePath)
      this.reportPath.foreach(p => Io.assertCanWriteFile(p, parentMustExist=false))

      Logger.level = this.logLevel

      val resources = TaskManagerResources(cores = cores.map(Cores(_)), totalMemory = memory.map(Memory(_)))
      this.taskManager = Some(new TaskManager(taskManagerResources=resources, scriptsDirectory = scriptsDirectory, logDirectory = logDirectory))
    }
    catch {
      case v: ValidationException => throw v
      case e: Exception => throw new ValidationException(DagrCoreMain.buildErrorMessage(msgOption = None, exceptionOption = Some(e)))
    }
  }

  /**
    * Attempts to setup the various directories needed to executed the pipeline, execute it, and generate
    * an execution report.
    */
  protected[cmdline] def execute(pipeline : Pipeline): Int = {
    val taskMan = this.taskManager.getOrElse(throw new IllegalStateException("execute() called before configure()"))
    val report  = this.reportPath.getOrElse(throw new IllegalStateException("execute() called before configure()"))

    taskMan.addTask(pipeline)
    taskMan.runAllTasks()

    // Write out the execution report
    val pw = new PrintWriter(Io.toWriter(report))
    TaskManager.logTaskStatusReport(taskMan, {str: String => pw.write(str + "\n")})
    pw.close()

    // return an exit code based on the number of non-completed tasks
    val taskInfoMap: BiMap[Task, TaskExecutionInfo] = taskMan.getTaskToInfoBiMap
    var numNotCompleted: Int = 0
    val iterator: Iterator[Task] = taskInfoMap.keys.toIterator
    var numCompleted: Int = iterator.size
    while (iterator.hasNext) {
      val taskInfo: TaskExecutionInfo = taskInfoMap.getValue(iterator.next).get
      if (TaskStatus.isTaskNotDone(taskInfo.status, failedIsDone=false)) {
        numNotCompleted += 1
        numCompleted -= 1
      }
    }

    numNotCompleted
  }
}
