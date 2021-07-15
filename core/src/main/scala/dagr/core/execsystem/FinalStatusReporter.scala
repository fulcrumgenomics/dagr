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

import java.nio.file.{Files, Path, Paths}
import java.time.format.DateTimeFormatter
import java.time.{Duration, Instant, ZoneId}

import com.fulcrumgenomics.commons.CommonsDef.DirPath
import com.fulcrumgenomics.commons.collection.BiMap
import com.fulcrumgenomics.commons.io.Io
import com.fulcrumgenomics.commons.util.StringUtil._
import com.fulcrumgenomics.commons.util.TimeUtil._
import dagr.core.DagrDef.TaskId
import dagr.core.execsystem.GraphNodeState.GraphNodeState
import dagr.core.execsystem.TaskStatus.TaskStatus
import dagr.core.tasksystem.Task

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/** Provides a method to provide an execution report for a task tracker */
trait FinalStatusReporter {
  this: TaskTracker =>

  /** The header for the report */
  private def reportHeader: List[String] = List(
    "ID", "NAME", "STATUS", "CORES", "MEMORY",
    "SUBMISSION_DATE", "START_DATE", "END_DATE",
    "EXECUTION_TIME", "TOTAL_TIME",
    "SCRIPT", "LOG", "ATTEMPT_INDEX", "DEBUG_STATE"
  )

  /** A row  for the report.  Each row is a given task. */
  private def reportRow(taskInfo: TaskExecutionInfo): List[String] = {
    // get the total execution time, and total time since submission
    val (executionTime: String, totalTime: String) = taskInfo.durationSinceStartAndFormat
    // The state of execution
    val graphNodeState = graphNodeStateFor(taskInfo.taskId).map(_.toString).getOrElse("NA")
    List(
      taskInfo.taskId.toString(),
      taskInfo.task.name,
      taskInfo.status.toString,
      f"${taskInfo.resources.cores.value}%.2f",
      taskInfo.resources.memory.prettyString,
      timestampStringOrNA(taskInfo.submissionDate),
      timestampStringOrNA(taskInfo.startDate),
      timestampStringOrNA(taskInfo.endDate),
      executionTime,
      totalTime,
      taskInfo.script.toFile.getAbsolutePath,
      taskInfo.logFile.toFile.getAbsolutePath,
      taskInfo.attemptIndex,
      graphNodeState).map(_.toString)
  }

  /** Writes a delimited string of the status of all tasks managed
    *
    * @param loggerMethod the method to use to write task status information, one line at a time
    * @param delimiter the delimiter between entries in a row
    */
  def logReport(loggerMethod: String => Unit, delimiter: String = "  "): Unit = {
    val taskInfoMap: BiMap[Task, TaskExecutionInfo] = taskToInfoBiMapFor

    // Create the task status table
    val taskStatusTable: ListBuffer[List[String]] = new ListBuffer[List[String]]()
    taskStatusTable.append(reportHeader)
    // Create a map to collect the counts for each task status
    val taskStatusMap: scala.collection.mutable.Map[TaskStatus.Value, Int] = mutable.HashMap[TaskStatus.Value, Int]()
    TaskStatus.values.foreach(status => taskStatusMap.put(status, 0))
    // Go through every task
    for (taskInfo <- taskInfoMap.values.toList.sortBy(taskInfo => taskInfo.taskId)) {
      // Make a report row
      taskStatusTable.append(reportRow(taskInfo))
      // Update the task status counts
      taskStatusMap.put(taskInfo.status, taskStatusMap.get(taskInfo.status).get + 1)
    }

    // Write the task status table
    loggerMethod(columnIt(taskStatusTable.toList, delimiter))

    // Create and write the task status counts
    val taskStatusCountTable = new ListBuffer[List[String]]()
    val keys = taskStatusMap.keys.toList.filter(status => taskStatusMap.getOrElse(status, 0) > 0)
    taskStatusCountTable.append(keys.map(_.toString))
    taskStatusCountTable.append(keys.map(status => taskStatusMap.getOrElse(status, 0).toString))
    loggerMethod("\n" + columnIt(taskStatusCountTable.toList, delimiter))
  }
}

/** Stores some useful information about a task, as read from an execution report.  */
case class TaskInfo
(
  report: Option[Path],
  id: TaskId,
  name: String,
  status: TaskStatus,
  cores: Cores,
  memory: Memory,
  submissionDate: Instant,
  startDate: Instant,
  endDate: Instant,
  executionTime: Duration,
  wallClockTime: Duration,
  script: Path,
  log: Path,
  attempts: Int,
  graphNodeState: GraphNodeState
) {
  /** True if this a [[Pipeline]], false if a [[UnitTask]]. */
  def isPipeline: Boolean = !log.toFile.exists()
  private val _id: Int = this.id.toInt
  override def hashCode(): Int = _id  // works, since ids should be unique, and speeds up this method!
}

object TaskInfo {
  private val TimeStampFormatter = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault())

  private def fromDate(date: String): Instant = Instant.from(TimeStampFormatter.parse(date))

  private def fromDuration(duration: String): Duration = {
    duration.split(':').map(_.toInt) match {
      case Array(hours, minutes, seconds) => Duration.ofSeconds(seconds + 60 * (minutes + 60 * hours))
      case _ => throw new IllegalArgumentException(s"Could not parse duration: $duration")
    }
  }

  /** Parses a line from an execution report. */
  def apply(line: String, report: Option[Path] = None, logsDir: Option[DirPath] = None): TaskInfo = {
    val fields = line.split("\\s{2,}")
    require(fields.length == 14, s"Expected 14 fields, found ${fields.length}: ${fields.toList}")

    val log    = Paths.get(fields(11)) match {
      case _log if Files.exists(_log) && !Files.isRegularFile(_log) && Files.isReadable(_log) => _log
      case _log => logsDir.map(_.resolve(_log.getFileName)).getOrElse(_log)
    }

    new TaskInfo(
      report         = report,
      id             = fields(0).toInt,
      name           = fields(1).replace(' ', '_'),
      status         = TaskStatus.withName(fields(2)),
      cores          = Cores(fields(3).toDouble),
      memory         = Memory(fields(4)),
      submissionDate = fromDate(fields(5)),
      startDate      = fromDate(fields(6)),
      endDate        = fromDate(fields(7)),
      executionTime  = fromDuration(fields(8)),
      wallClockTime  = fromDuration(fields(9)),
      script         = Paths.get(fields(10)),
      log            = log,
      attempts       = fields(12).toInt,
      graphNodeState = GraphNodeState.withName(fields(13))
    )
  }

  /** Slurps in the lines from an execution report. */
  def from(report: Path, logsDir: Option[DirPath] = None): Iterator[TaskInfo] = {
    Io.readLines(report)
      .drop(1) // header line
      .map(_.trim) // leading and trailing whitespace exist
      .takeWhile(_.nonEmpty) // hacky way of finding the end of the file
      .filterNot(_.contains(" NA ")) // hacky way to find tasks with no start date
      .map(line => TaskInfo(line, report=Some(report), logsDir=logsDir))
  }
}
