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
 *
 */

package dagr.core.reporting

import java.io.{ByteArrayOutputStream, PrintStream}

import com.fulcrumgenomics.commons.CommonsDef.yieldAndThen
import com.fulcrumgenomics.commons.util.Logger
import com.fulcrumgenomics.commons.util.StringUtil._
import com.fulcrumgenomics.commons.util.TimeUtil._
import dagr.api.models.ResourceSet
import dagr.core.exec.ExecDef.concurrentSet
import dagr.core.exec.{Executor, SystemResources}
import dagr.core.execsystem.TaskManager
import dagr.core.execsystem2.GraphExecutor
import dagr.core.reporting.ReportingDef.TaskLogger
import dagr.core.tasksystem.Task.TaskInfoLike
import dagr.core.tasksystem.{InJvmTask, ProcessTask, Task, UnitTask}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.tools.jline_embedded.{TerminalFactory, Terminal => JLineTerminal}

/** Simple methods for a terminal */
object Terminal {
  case class Dimensions(width: Int, height: Int)
  def supportsAnsi: Boolean = terminal.isAnsiSupported
  /** Gets a new terminal for each call, in case the terminal is re-sized. */
  protected[reporting] def terminal: JLineTerminal = TerminalFactory.get()
}

/** A trait with methods for asking questions about a command line terminal. */
trait Terminal {
  import Terminal.Dimensions

  def dimensions: Dimensions = {
    val t = Terminal.terminal
    Dimensions(t.getWidth, t.getHeight)
  }

  def supportsAnsi: Boolean = Terminal.terminal.isAnsiSupported
}

/** ANSI Strings for cursor movements.
  *
  * See: http://www.tldp.org/HOWTO/Bash-Prompt-HOWTO/x361.html
  */
object CursorMovement {
  def positionCursor(line: Int, column: Int): String = s"\033[$line;${column}H"
  //def moveUp(numLines: Int): String = s"\033[${numLines}A"
  //def moveDown(numLines: Int): String = s"\033[${numLines}B"
  //def moveRight(numColumns: Int): String = s"\033[${numColumns}C"
  //def moveLeft(numColumns: Int): String = s"\033[${numColumns}D"
  def clearScreen: String = "\033[2J"
  def eraseToEOL: String = "\033[K"
}

object TopLikeStatusReporter {
  /** Truncates the line if it is too long and appends "...", otherwise returns the line */
  private def truncateLine(line: String, maxColumns: Int): String = if (maxColumns < line.length) line.substring(0, line.length-3) + "..." else line

  /** Splits the string by newlines, and truncates each line if necessary.  Enforces a maximum number of lines. */
  private def wrap(output: String, maxLines: Int, maxColumns: Int): String = {
    var lines = output.split("\n")
    lines = if (maxLines <= lines.length) {
      val numLinesLeft = lines.length - (maxLines-1)
      lines.slice(0, maxLines-1) ++ List(s"... with $numLinesLeft more lines not shown ...")
    }
    else {
      lines
    }
    lines.map(truncateLine(_, maxColumns)).mkString("\n")
  }

  /** The various columns to report. */
  object Column extends Enumeration {
    type Column = Value
    val Id, Name, Status, Cores, Memory,
    SubmissionDate, StartDate, EndDate,
    ExecutionTime, TotalTime, AttemptIndex
    = Value

    // See: http://stackoverflow.com/questions/2559759/how-do-i-convert-camelcase-into-human-readable-names-in-java
    def format(column: Column): String = {
      column.toString.replaceAll(
        String.format("%s|%s|%s",
          "(?<=[A-Z])(?=[A-Z][a-z])",
          "(?<=[^A-Z])(?=[A-Z])",
          "(?<=[A-Za-z])(?=[^A-Za-z])"
        ),
        " "
      )
    }
  }

  /** Creates a new [[TopLikeStatusReporter]] specific to the given executor.  Will add itself to the list of loggers to
    * be notified by a change in status in [[TaskInfoLike]]. */
  def apply(executor: Executor): TopLikeStatusReporter = {
    val loggerOutputStream: ByteArrayOutputStream = {
      val outStream = new ByteArrayOutputStream()
      val printStream = new PrintStream(outStream)
      Logger.out = printStream
      outStream
    }

    val logger = executor match {
      case taskManager: TaskManager      =>
          new dagr.core.execsystem.TopLikeStatusReporter(
            taskManager = taskManager,
            loggerOut   = Some(loggerOutputStream),
            print       = s => System.out.print(s)
          )
      case graphExecutor: GraphExecutor[_] =>
        new dagr.core.execsystem2.TopLikeStatusReporter(
          systemResources = graphExecutor.resources.getOrElse(throw new IllegalArgumentException("No resource set defined")),
          loggerOut       = Some(loggerOutputStream),
          print           = s => System.out.print(s)
        )
      case _ => throw new IllegalArgumentException(s"Unknown executor: '${executor.getClass.getSimpleName}'")
    }

    // Register!
    yieldAndThen(logger)(executor.withReporter(logger))
  }
}


/** A top-like status reporter that prints execution information to the terminal. The
  * [[TopLikeStatusReporter#refresh()]] method is used to refresh the terminal.  Currently
  * only displays tasks the extend [[UnitTask]]. */
trait TopLikeStatusReporter extends TaskLogger with Terminal {
  import TopLikeStatusReporter.Column.{Column => Field}
  import TopLikeStatusReporter.{Column, _}

  /** All the tasks we have ever known */
  private val _tasks:  mutable.Set[Task] = concurrentSet()

  // refresh initially
  this.refresh(loggerOut=loggerOut, print=print)

  /** The set of all tests about which are currently known */
  protected def tasks: Traversable[Task] = _tasks

  /** A stream from which to read log messages. */
  protected def loggerOut: Option[ByteArrayOutputStream]

  /** The method use to print the status. */
  protected def print: String => Unit

  /** the total system resources */
  protected def systemResources: SystemResources

  /** True if the task is running, false otherwise. */
  protected def running(task: Task): Boolean

  /** True if the task is ready for execution (no dependencies), false otherwise. */
  protected def queued(task: Task): Boolean

  /** True if the task has failed, false otherwise. */
  protected def failed(task: Task): Boolean

  /** True if the task has succeeded, false otherwise. */
  protected def succeeded(task: Task): Boolean

  /** True if the task has completed regardless of status, false otherwise. */
  protected def completed(task: Task): Boolean

  /** True if the task has unmet dependencies, false otherwise. */
  protected def pending(task: Task): Boolean

  /** The information for a single task.  See [[TopLikeStatusReporter.Column]] for the list of
    * information being returned. */
  private def taskInfoLine(info: Task.TaskInfo): Map[Field, String] = {
    // get the total execution time, and total time since submission
    val (executionTime: String, totalTime: String) = info.executionAndTotalTime
    // The state of execution
    Map[Field, Any](
      Column.Id             -> info.id.getOrElse(BigInt(-1)),
      Column.Name           -> info.task.name,
      Column.Status         -> info.status.toString,
      Column.Cores          -> f"${info.resources.map(_.cores.value).getOrElse(0.0)}%.2f",
      Column.Memory         -> info.resources.map(_.memory.prettyString).getOrElse("NA"),
      Column.SubmissionDate -> timestampStringOrNA(info.submissionDate),
      Column.StartDate      -> timestampStringOrNA(info.startDate),
      Column.EndDate        -> timestampStringOrNA(info.endDate),
      Column.ExecutionTime  -> executionTime,
      Column.TotalTime      -> totalTime,
      Column.AttemptIndex   -> info.attempts
    ).map { case (k, v) => (k, v.toString) }
  }

  /** Writes a delimited string of the status of all tasks managed to the console.
    *
    * @param print the method to use to write task status information, one line at a time.
    * @param loggerOut the stream to which log messages are written, or none if no stream is available.
    * @param delimiter the delimiter between entries in a row.
    */
  def refresh(print: String => Unit, loggerOut: Option[ByteArrayOutputStream] = None, delimiter: String = "  "): Unit = {
    val systemResources = this.systemResources
    var numLinesLeft = dimensions.height

    // Erase the screen, line-by-line.
    for (i <- 0 until dimensions.height) {
      print(CursorMovement.positionCursor(i, 0))
      print(CursorMovement.eraseToEOL)
    }
    print(CursorMovement.positionCursor(0, 0))

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Header row
    //////////////////////////////////////////////////////////////////////////////////////////////
    val header = new mutable.StringBuilder()

    val unitTasks: Seq[UnitTask]    = tasks.toSeq.collect { case t: UnitTask => t }
    val runningTasks: Seq[UnitTask] = unitTasks.filter(running)

    def sumCores(tasks: Traversable[Task]): Double = tasks.flatMap(_.taskInfo.resources).map(_.cores.value).sum
    def sumMemory(tasks: Traversable[Task]): Long = tasks.flatMap(_.taskInfo.resources).map(_.memory.value).sum

    val systemCoresPct: Double  = 100.0 * sumCores(runningTasks)                                       / systemResources.cores.value
    val systemMemoryPct: Double = 100.0 * sumMemory(runningTasks.collect { case t: ProcessTask => t }) / systemResources.systemMemory.value
    val jvmMemoryPct: Double    = 100.0 * sumMemory(runningTasks.collect { case t: InJvmTask => t })   / systemResources.jvmMemory.value

    // NB: only considers unit tasks
    val numRunning    = runningTasks.size
    val numEligible   = tasks.count(queued)
    val numFailed     = unitTasks.count(failed)
    val numDone       = unitTasks.count(succeeded)
    val numIneligible = unitTasks.count(pending)

    // Basic info about the total resources being used
    header.append(f"Cores:         ${systemResources.cores.value}%6.1f, $systemCoresPct%.1f%% Used\n")
    header.append(f"System Memory: ${systemResources.systemMemory.prettyString}%6s, $systemMemoryPct%.1f%% Used\n")
    header.append(f"JVM Memory:    ${systemResources.jvmMemory.prettyString}%6s, $jvmMemoryPct%.1f%% Used\n")
    header.append(f"Unit Tasks:    $numRunning Running, $numDone Done, $numFailed Failed, $numEligible Eligible, $numIneligible Ineligible")

    // Add the last log message
    loggerOut.map(_.toString).map(str => str.split("\n").last).foreach(msg => header.append(f"\nLog Message:   $msg"))

    print(wrap(header.toString, maxLines=numLinesLeft, maxColumns=dimensions.width) + "\n")
    numLinesLeft -= header.count(_ == '\n') + 1

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Write the task status table
    //////////////////////////////////////////////////////////////////////////////////////////////

    // Create the task status table
    val tableTasks: ListBuffer[Task] = new ListBuffer[Task]()

    // Gets a list of tasks to add to the table of tasks.  Uses the filter predicate to select only
    // a particular set of tasks.  Tasks will be sorted by those having resources first, then secondly by
    // the task id (infinite if none assigned)
    def filterTasks(filter: Task => Boolean): Seq[Task] = {
      if (tableTasks.size < numLinesLeft) {
        val moreTasks = tasks.filter(filter).toSeq
          .sortBy { info =>
            (info.taskInfo.resources.isEmpty, info.taskInfo.id.getOrElse(BigInt(Int.MaxValue)))
          }
        if (moreTasks.size + tableTasks.size <= numLinesLeft) moreTasks
        else moreTasks.slice(0, numLinesLeft - tableTasks.size)
      }
      else {
        Seq.empty
      }
    }

    if (0 < numLinesLeft) { print("\n"); numLinesLeft -= 1 }

    // Get tasks that are running
    tableTasks ++= filterTasks(running)
    // Get tasks that have no unmet dependencies
    tableTasks ++= filterTasks(queued)
    // Add those that failed if we have more lines
    tableTasks ++= filterTasks(failed)
    // Add those that completed if we have more lines
    tableTasks ++= filterTasks(completed)
    // Add those that have unmet dependencies if we have more lines
    tableTasks ++= filterTasks(pending)

    // Now generate a row (line) per task info
    val taskStatusTable: ListBuffer[List[String]] = new ListBuffer[List[String]]()
    // header
    taskStatusTable += Column.values.toList.map(field => Column.format(field))
    // tasks
    taskStatusTable ++= tableTasks
      .map(_.taskInfo)
      .map { taskInfo =>
        val rowInfo = taskInfoLine(taskInfo)
        Column.values.toList.map { field =>
          rowInfo.get(field) match {
            case Some(v) => v
            case None => throw new IllegalArgumentException("Field missing from report row: " + field)
          }
        }
      }

    if (1 < taskStatusTable.size) {
      print(wrap(columnIt(taskStatusTable.toList, delimiter), maxLines = numLinesLeft, maxColumns = dimensions.width) + "\n")
      numLinesLeft -= taskStatusTable.count(_ == '\n') + 1
    }
  }

  /** This method is called when any info about a task is updated. */
  final def record(info: TaskInfoLike): Unit = {
    // add the task to the set of known tasks
    this._tasks += info.task
    // refresh the screen
    this.refresh(loggerOut=loggerOut, print=print)
  }
}
