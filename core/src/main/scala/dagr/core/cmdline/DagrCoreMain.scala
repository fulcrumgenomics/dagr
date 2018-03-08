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
import java.net.InetAddress
import java.nio.file.{Files, Path}
import java.text.DecimalFormat

import com.fulcrumgenomics.commons.CommonsDef.{FilePath, unreachable}
import com.fulcrumgenomics.commons.io.{Io, PathUtil}
import com.fulcrumgenomics.commons.util.{LazyLogging, LogLevel, Logger}
import com.fulcrumgenomics.sopt.cmdline.{CommandLineProgramParserStrings, ValidationException}
import com.fulcrumgenomics.sopt.parsing.{ArgOptionAndValues, ArgTokenCollator, ArgTokenizer}
import com.fulcrumgenomics.sopt.util.TermCode
import com.fulcrumgenomics.sopt.{Sopt, arg}
import dagr.core.config.Configuration
import dagr.core.exec._
import dagr.core.reporting.{ReplayLogger, Terminal, TopLikeStatusReporter}
import dagr.core.tasksystem.Pipeline
import dagr.api.models.util.{Cores, Memory}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.Success


object DagrCoreMain extends Configuration {
  /** The packages we wish to include in our command line **/
  protected def getPackageList: List[String] = {
    val config = new Configuration {}
    config.optionallyConfigure[List[String]](Configuration.Keys.PackageList) getOrElse List[String]("dagr")
  }

  /** The main method */
  def main(args: Array[String]): Unit = {
    new DagrCoreMain[DagrCoreArgs]().makeItSoAndExit(args)
  }

  /** Provide a command line validation error message */
  private[cmdline] def buildErrorMessage(msgOption: Option[String] = None, exceptionOption: Option[Exception] = None): String = {
    val extraMsg = msgOption map { text => s"\nmessage: $text" } getOrElse ""
    exceptionOption match {
      case Some(e) =>
        s"${e.getClass.getCanonicalName}: ${this.commandLineName()} command line validation error: ${e.getMessage}$extraMsg"
      case None =>
        s"${this.commandLineName()} command line validation error$extraMsg"
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
class DagrCoreArgs(
// TODO: update args with precedence information
  @arg(doc = "Load in a custom configuration into Dagr.  See https://github.com/typesafehub/config for details on the file format.")
  val config: Option[Path] = None,
  @arg(doc = "Stop pipelines immediately on detecting the first failed task.")
  val failFast: Boolean = false,
  @arg(doc = "Overrides the default scripts directory in the configuration file.")
  val scriptDir: Option[Path] = None,
  @arg(doc = "Overrides default log directory in the configuration file.")
  val logDir: Option[Path] = None,
  @arg(doc = "Set the logging level.", flag='l')
  val logLevel: LogLevel = LogLevel.Info,
  @arg(doc = "Dagr scala scripts to compile and add to the list of programs (must end with .dagr or .scala).", minElements=0)
  val scripts: List[Path] = Nil,
  @arg(doc = "Set the number of cores available to dagr.")
  val cores: Option[Double] = None,
  @arg(doc = "Set the memory available to dagr.")
  val memory: Option[String] = None,
  @arg(doc = "Write an execution report to this file, otherwise write to the stdout")
  val report: Option[Path] = None,
  @arg(doc = "Provide an top-like interface for tasks with the give delay in seconds. This suppress info logging.")
  var interactive: Boolean = false,
  @arg(doc = "Use the experimental execution system.")
  val experimentalExecution: Boolean = false,
  @arg(doc = "Attempt to replay using the provided replay log")
  val replayLog: Option[FilePath] = None
) extends LazyLogging {

  // These are not optional, but are only populated during configure()
  private var executor   : Option[Executor] = None
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
    catch { case _: Exception => errors += DagrCoreMain.buildErrorMessage(Some(s"Could not create the $use directory: $dir")) }
  }

  // Invoked by DagrCommandLineParser after the pipeline has also been instantiated
  protected[dagr] def configure(pipeline: Pipeline, commandLine: Option[String] = None)(implicit ex: ExecutionContext) : Unit = {
    try {
      val config = new Configuration { }

      // scripts & logs directories
      val scriptDirectory = pick(this.scriptDir, pipeline.outputDirectory.map(_.resolve("scripts")),
        config.optionallyConfigure(Configuration.Keys.ScriptDirectory), Some(Io.makeTempDir("scripts")))
      val logDirectory    = pick(this.logDir,    pipeline.outputDirectory.map(_.resolve("logs")),
        config.optionallyConfigure(Configuration.Keys.LogDirectory), Some(Io.makeTempDir("logs")))

      {
        val errors: ListBuffer[String] = ListBuffer[String]()
        scriptDirectory.foreach(d => mkdir(dir=d, use="scripts", errors=errors))
        logDirectory.foreach(d => mkdir(dir=d, use="logs", errors=errors))
        if (errors.nonEmpty) throw ValidationException(errors.toList)
      }

      Logger.level = this.logLevel

      // report file
      this.reportPath = pick(report, pipeline.outputDirectory.map(_.resolve("execution_report.txt")), Option(Io.StdOut)).map(_.toAbsolutePath)
      this.reportPath.foreach(p => Io.assertCanWriteFile(p, parentMustExist=false))

      val resources = SystemResources(cores = cores.map(Cores(_)), totalMemory = memory.map(Memory(_)))
      this.executor = Some(
        Executor(
          experimentalExecution = experimentalExecution,
          resources             = resources,
          scriptDirectory       = scriptDirectory.get,
          logDirectory          = logDirectory.get
        )
      )

      // Print all the arguments if desired.
      commandLine.foreach { line =>
        config.optionallyConfigure[Boolean](Configuration.Keys.PrintArgs).filter(x => x).foreach { _ =>
          logger.info("Execution arguments: " + line)
        }
      }
    }
    catch {
      case v: ValidationException => throw v
      case e: Exception => throw new ValidationException(DagrCoreMain.buildErrorMessage(msgOption = None, exceptionOption = Some(e)))
    }
  }

  protected def executeSetup(executor: Executor, report: FilePath)(implicit ex: ExecutionContext): Unit = {
    // Set up an interactive logger if desired and supported
    if (this.interactive) {
      if (Terminal.supportsAnsi) {
        executor.withReporter(TopLikeStatusReporter(executor))
      }
      else {
        logger.warning("ANSI codes are not supported in your terminal.  Interactive mode will not be used.")
      }
    }

    // Set up the execution logger whose output can be used later for replay
    {
      val logName = "replay_log.csv"
      val log = if (Seq(Io.StdOut, PathUtil.pathTo("/dev/stderr"), Io.DevNull).contains(report)) {
        executor.logDir.resolve(logName)
      }
      else {
        report.getParent.resolve(logName)
      }
      val executionLogger = new ReplayLogger(log)
      executor.withReporter(executionLogger)
    }

    // Set up the task cache (in case of replay)
    this.replayLog.foreach { log =>
      executor.withReporter(TaskCache(log))
    }
  }

  protected def executeFinish(executor: Executor, report: FilePath): Unit = {
    // Write out the execution report
    if (!interactive || Io.StdOut != report) {
      val pw = new PrintWriter(Io.toWriter(report))
      executor.logReport({ str: String => pw.write(str + "\n") })
      pw.close()
    }

  }

  /**
    * Attempts to setup the various directories needed to executed the pipeline, execute it, and generate
    * an execution report.
    */
  protected[cmdline] def execute(pipeline : Pipeline)(implicit ex: ExecutionContext): Int = {
    val report = this.reportPath.getOrElse(throw new IllegalStateException("execute() called before configure()"))

    // Get the executor
    val executor = this.executor.getOrElse(throw new IllegalStateException("Executor was not configured, did you all configure()"))

    // Set up any task prior to execution
    executeSetup(executor, report)

    // execute
    val exitCode = executor.execute(pipeline)

    // complete any shutdown tasks after execution
    executeFinish(executor, report)

    exitCode
  }
}

class DagrCoreMain[Args<:DagrCoreArgs:TypeTag:ClassTag] extends LazyLogging {
  protected def name: String = "dagr"

  /** A main method that invokes System.exit with the exit code. */
  def makeItSoAndExit(args: Array[String]): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global
    System.exit(makeItSo(args))
  }

  /** A main method that returns an exit code instead of exiting. */
  def makeItSo(args: Array[String], packageList: List[String] = DagrCoreMain.getPackageList, includeHidden: Boolean = false)
              (implicit ex: ExecutionContext): Int = {
    // Initialize color options
    TermCode.printColor = DagrCoreMain.optionallyConfigure[Boolean](Configuration.Keys.ColorStatus).getOrElse(true)

    // Load any dagr scripts - assumes the option is "--scripts" only
    loadScripts(args)

    val startTime = System.currentTimeMillis()
    val packages = Sopt.find[Pipeline](packageList, includeHidden=includeHidden)
    val exit      = Sopt.parseCommandAndSubCommand[Args,Pipeline](name, args, packages) match {
      case Sopt.Failure(usage) =>
        System.err.print(usage())
        1
      case Sopt.CommandSuccess(_) =>
        unreachable("CommandSuccess should never be returned by parseCommandAndSubCommand.")
      case Sopt.SubcommandSuccess(dagr, pipeline) =>
        val name = pipeline.getClass.getSimpleName
        try {
          dagr.configure(pipeline, Some(args.mkString(" ")))
          val name = Configuration.commandLineName(this.name)
          printStartupLines(name, args)
          val numFailed = dagr.execute(pipeline)
          printEndingLines(startTime, name, success = numFailed == 0)
          numFailed
        }
        catch {
          case ex: Throwable =>
            printEndingLines(startTime, name, success=false)
            throw ex
        }
    }

    exit
  }

  /** Prints a line of useful information when a tool starts executing. */
  protected def printStartupLines(tool: String, args: Array[String]): Unit = {
    val version    = CommandLineProgramParserStrings.version(getClass, color=false).replace("Version: ", "")
    val host       = InetAddress.getLocalHost.getHostName
    val user       = System.getProperty("user.name")
    val jreVersion = System.getProperty("java.runtime.version")
    logger.info(s"Executing $tool from $name version $version as $user@$host on JRE $jreVersion")
  }

  /** Prints a line of useful information when a tool stops executing. */
  protected def printEndingLines(startTime: Long, name: String, success: Boolean): Unit = {
    val elapsedMinutes: Double = (System.currentTimeMillis() - startTime) / (1000d * 60d)
    val elapsedString: String = new DecimalFormat("#,##0.00").format(elapsedMinutes)
    val verb = if (success) "completed" else "failed"
    logger.info(s"$name $verb. Elapsed time: $elapsedString minutes.")
  }

  /** Loads the various dagr scripts and puts them on the classpath. */
  private def loadScripts(args: Array[String]): Unit = {
    val tokenizer = new ArgTokenizer(args, argFilePrefix=Some("@"))
    val collator = new ArgTokenCollator(tokenizer)
    collator.filter(_.isSuccess).foreach {
      case Success(ArgOptionAndValues(name: String, values: Seq[String])) if name == "scripts" =>
        def isScript(name: String): Boolean = name.endsWith(".dagr") || name.endsWith(".scala")
        val scripts = values.filter(isScript).map { name =>
          val path = PathUtil.pathTo(name)
          Io.assertReadable(path)
          path
        }
        new DagrScriptManager().loadScripts(
          scripts,
          Files.createTempDirectory(PathUtil.pathTo(System.getProperty("java.io.tmpdir")), "dagrScripts"),
          quiet = false
        )
      case _ => false
    }
  }
}
