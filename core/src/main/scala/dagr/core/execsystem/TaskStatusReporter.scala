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

import java.time.{ZoneId, Duration}
import java.time.format.DateTimeFormatter
import java.time.temporal.Temporal

import dagr.core.tasksystem.Task
import dagr.core.util.BiMap
import dagr.core.util.StringUtil._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** Provides a method to provide an execution report for a task tracker */
trait TaskStatusReporter {
  this: TaskTracker =>

  private val fmt = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault())

  /** Gets the time stamp as a string (without Nanoseconds), or NA if it is None */
  private def timestampStringOrNA(timestamp: Option[Temporal]): String = {
    timestamp match {
      case Some(ts) => fmt.format(ts)
      case None => "NA"
    }
  }

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
    val (executionTime: String, totalTime: String) = (taskInfo.submissionDate, taskInfo.startDate, taskInfo.endDate) match {
      case (Some(submission), Some(start), Some(end)) =>
        val sinceSubmission = Duration.between(submission, end)
        val sinceStart      = Duration.between(start, end)
        (formatElapsedTime(sinceStart.getSeconds), formatElapsedTime(sinceSubmission.getSeconds))
      case _ => ("NA", "NA")
    }
    // The state of execution
    val graphNodeState = graphNodeStateFor(taskInfo.taskId) match {
      case Some(state) => state.toString
      case None => "NA"
    }
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
