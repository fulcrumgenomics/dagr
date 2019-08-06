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

package dagr.core.execsystem

import java.io.ByteArrayOutputStream
import java.util.concurrent.atomic.AtomicBoolean

import com.fulcrumgenomics.commons.collection.BiMap
import com.fulcrumgenomics.commons.util.StringUtil._
import com.fulcrumgenomics.commons.util.TimeUtil._
import dagr.core.execsystem.GraphNodeState._
import dagr.core.execsystem.TopLikeStatusReporter.{ReportField, StatusRunnable}
import dagr.core.tasksystem.{InJvmTask, ProcessTask, Task, UnitTask}
import jline.{TerminalFactory, Terminal => JLineTerminal}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** Simple methods for a terminal */
object Terminal {
  case class Dimensions(width: Int, height: Int)
  def supportsAnsi: Boolean = terminal.isAnsiSupported
  /** Gets a new terminal for each call, in case the terminal is re-sized. */
  protected[execsystem] def terminal: JLineTerminal = TerminalFactory.get()
}

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
  def positionCursor(line: Int, column: Int): String = s"\u001b$line;${column}H"
  //def moveUp(numLines: Int): String = s"\033[${numLines}A"
  //def moveDown(numLines: Int): String = s"\033[${numLines}B"
  //def moveRight(numColumns: Int): String = s"\033[${numColumns}C"
  //def moveLeft(numColumns: Int): String = s"\033[${numColumns}D"
  def clearScreen: String = "\u001b[2J"
  def eraseToEOL: String = "\u001b[K"
}

object TopLikeStatusReporter {
  /** Refreshes the top-like interface until terminate is called.
    *
    * @param reporter the reporter which to refresh.
    * @param loggerOut the stream to which log messages are written, or none if no stream is available.
    * @param refreshRate the refresh rate in ms.
    */
  private class StatusRunnable(reporter: TopLikeStatusReporter,
                               loggerOut: Option[ByteArrayOutputStream] = None,
                               refreshRate: Int = 1000,
                               print: String => Unit = print) extends Runnable {
    private val keepPrinting: AtomicBoolean = new AtomicBoolean(true)
    def terminate(): Unit = keepPrinting.set(false)
    override def run(): Unit = {
      // skip over any current output
      print(CursorMovement.clearScreen)
      // keep printing until told otherwise
      while (keepPrinting.get()) {
        reporter.refresh(print=print, loggerOut)
        Thread.sleep(refreshRate)
      }
    }
  }
  
  object ReportField extends Enumeration {
    type ReportField = Value
    val Id, Name, Status, State, Cores, Memory,
    SubmissionDate, StartDate, EndDate,
    ExecutionTime, TotalTime, AttemptIndex
    = Value

    // See: http://stackoverflow.com/questions/2559759/how-do-i-convert-camelcase-into-human-readable-names-in-java
    def format(field: ReportField): String = {
      field.toString.replaceAll(
        String.format("%s|%s|%s",
          "(?<=[A-Z])(?=[A-Z][a-z])",
          "(?<=[^A-Z])(?=[A-Z])",
          "(?<=[A-Za-z])(?=[^A-Za-z])"
        ),
        " "
      )
    }

  }

  /** The amount of time in milliseconds to wait for the thread to join */
  private val ShutdownJoinTime = 5000
}

/** Mix this trait into a [[dagr.core.execsystem.TaskManager]] to provide the `refresh()` method for a
  * top-like status interface. */
class TopLikeStatusReporter(taskManager: TaskManager,
                            loggerOut: Option[ByteArrayOutputStream] = None,
                            print: String => Unit = print) extends Terminal {
  import TopLikeStatusReporter.ReportField.ReportField

  private val terminalRunnable = new StatusRunnable(reporter=this, loggerOut=loggerOut, print=print)
  private val terminalThread = new Thread(terminalRunnable)
  private var lastLogMessage: Option[String] = None

  /** Start the top like status thread */
  def start(): Unit = {
    terminalThread.setDaemon(true)
    this.refresh(print=print)
    terminalThread.start()
  }

  /** Shutdown the top like status thread */
  def shutdown(): Unit = {
    terminalRunnable.terminate()
    terminalThread.join(TopLikeStatusReporter.ShutdownJoinTime)
    this.refresh(print=print)
    print("\n")
  }

  /** Truncates the line if it is too long and appends "...", otherwise returns the line */
  private def truncateLine(line: String, maxColumns: Int): String = if (maxColumns < line.length) line.substring(0, line.length-3) + "..." else line

  /** Splits the string by newlines, and truncates each line if necessary.  Enforces a maximum number of lines. */
  private def wrap(output: String, maxLines: Int, maxColumns: Int): String = {
    var lines = output.split("\n")
    lines = if (maxLines <= lines.length) {
      val numLinesLeft = lines.length - (maxLines-2)
      lines.slice(0, maxLines-2) ++ List(s"... with $numLinesLeft more lines not shown ...")
    }
    else {
      lines
    }
    lines.map(truncateLine(_, maxColumns)).mkString("\n")
  }

  /** A row for the report.  Each row is a single task. . */
  private def reportRow(taskInfo: TaskExecutionInfo): Map[ReportField, String] = {
    // get the total execution time, and total time since submission
    val (executionTime: String, totalTime: String) = taskInfo.durationSinceStartAndFormat
    // The state of execution
    val graphNodeState = taskManager.graphNodeStateFor(taskInfo.taskId).map(_.toString).getOrElse("NA")
    Map[ReportField, Any](
      ReportField.Id             -> taskInfo.taskId,
      ReportField.Name           -> taskInfo.task.name,
      ReportField.Status         -> taskInfo.status.toString,
      ReportField.State          -> graphNodeState,
      ReportField.Cores          -> f"${taskInfo.resources.cores.value}%.2f",
      ReportField.Memory         -> taskInfo.resources.memory.prettyString,
      ReportField.SubmissionDate -> timestampStringOrNA(taskInfo.submissionDate),
      ReportField.StartDate      -> timestampStringOrNA(taskInfo.startDate),
      ReportField.EndDate        -> timestampStringOrNA(taskInfo.endDate),
      ReportField.ExecutionTime  -> executionTime,
      ReportField.TotalTime      -> totalTime,
      ReportField.AttemptIndex   -> taskInfo.attemptIndex
    ).map { case (k, v) => (k, v.toString) }
  }

  /** Writes a delimited string of the status of all tasks managed to the console.
    *
    * @param print the method to use to write task status information, one line at a time.
    * @param delimiter the delimiter between entries in a row.
    */
  private[core] def refresh(print: String => Unit, loggerOut: Option[ByteArrayOutputStream] = None, delimiter: String = "  "): Unit = {
    val taskInfoMap: BiMap[Task, TaskExecutionInfo] = taskManager.taskToInfoBiMapFor
    val taskManagerResources = taskManager.getTaskManagerResources
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

    val unitTasks = taskInfoMap.filter { case (task, taskInfo) => task.isInstanceOf[UnitTask]}.map { case (task, taskInfo) => taskInfo }
    val runningTasks: Map[UnitTask, ResourceSet] = taskManager.runningTasksMap

    val systemCoresPct: Double  = 100.0 * runningTasks.values.map(_.cores.value).sum / taskManagerResources.cores.value
    val systemMemoryPct: Double = 100.0 * runningTasks.filterKeys(_.isInstanceOf[ProcessTask]).values.map(_.memory.value).sum / taskManagerResources.systemMemory.value
    val jvmMemoryPct: Double    = 100.0 * runningTasks.filterKeys(_.isInstanceOf[InJvmTask]).values.map(_.memory.value).sum / taskManagerResources.jvmMemory.value

    // NB: only considers unit tasks
    val numRunning    = runningTasks.size
    val numEligible   = taskManager.readyTasksList.size
    val numFailed     = unitTasks.count(taskInfo => TaskStatus.isTaskFailed(taskInfo.status))
    val numDone       = unitTasks.count(taskInfo => TaskStatus.isTaskDone(taskInfo.status, failedIsDone=false))
    val numIneligible = unitTasks.filter(taskInfo => taskInfo.status == TaskStatus.UNKNOWN).count(taskInfo => taskManager.graphNodeStateFor(taskInfo.taskId).exists(_ != NO_PREDECESSORS))
    loggerOut.map(_.toString).map(str => str.split("\n").last).foreach(msg => lastLogMessage = Some(msg))

    header.append(f"Cores:         ${taskManagerResources.cores.value}%6.1f, $systemCoresPct%.1f%% Used\n")
    header.append(f"System Memory: ${taskManagerResources.systemMemory.prettyString}%6s, $systemMemoryPct%.1f%% Used\n")
    header.append(f"JVM Memory:    ${taskManagerResources.jvmMemory.prettyString}%6s, $jvmMemoryPct%.1f%% Used\n")
    header.append(f"Unit Tasks:    $numRunning Running, $numDone Done, $numFailed Failed, $numEligible Eligible, $numIneligible Ineligible\n")
    lastLogMessage.foreach(msg => header.append(f"Log Message:   $msg"))

    print(wrap(header.toString, maxLines=numLinesLeft, maxColumns=dimensions.width) + "\n")
    numLinesLeft -= header.count(_ == '\n') + 1

    //////////////////////////////////////////////////////////////////////////////////////////////
    // Write the task status table
    //////////////////////////////////////////////////////////////////////////////////////////////

    if (0 < numLinesLeft) { print("\n"); numLinesLeft -= 1 }

    // Create the task status table
    val infos: ListBuffer[TaskExecutionInfo] = new ListBuffer[TaskExecutionInfo]()

    // Get tasks that have started or have no unmet dependencies
    infos ++= taskInfoMap
      .values
      .toList
      .filter { taskInfo => taskInfo.status == TaskStatus.STARTED || taskManager.graphNodeFor(taskInfo.taskId).get.state == GraphNodeState.NO_PREDECESSORS}

    // Add those that failed if we have more lines
    if (infos.size < numLinesLeft) {
      val moreInfos = taskInfoMap
        .values
        .toList
        .filter { taskInfo => TaskStatus.isTaskFailed(taskInfo.status) }
        .sortBy(taskInfo => taskInfo.taskId)
      if (moreInfos.size + infos.size <= numLinesLeft) infos ++= moreInfos
      else infos ++= moreInfos.slice(0, numLinesLeft - infos.size)
    }

    // Add those that completed if we have more lines
    if (infos.size < numLinesLeft) {
      val moreInfos = taskInfoMap
        .values
        .toList
        .filter { taskInfo =>  TaskStatus.isTaskDone(taskInfo.status, failedIsDone=false) }
        .sortBy(taskInfo => taskInfo.taskId)
      if (moreInfos.size + infos.size <= numLinesLeft) infos ++= moreInfos
      else infos ++= moreInfos.slice(0, numLinesLeft - infos.size)
    }

    // Now generate a row (line) per task info
    val taskStatusTable: ListBuffer[List[String]] = new ListBuffer[List[String]]()
    taskStatusTable += ReportField.values.toList.map(field => ReportField.format(field))
    taskStatusTable ++= infos
      .sortBy(taskInfo => taskInfo.taskId)
      .sortBy(taskInfo => taskManager.graphNodeFor(taskInfo.taskId).get.state)
      .sortBy(taskInfo => taskInfo.status)
      .map {
        taskInfo =>
          val row = reportRow(taskInfo)
          ReportField.values.toList.map {
            field =>
              row.get(field) match {
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
}
