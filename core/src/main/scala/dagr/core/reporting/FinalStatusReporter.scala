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

import java.time.Instant

import com.fulcrumgenomics.commons.util.SimpleCounter
import com.fulcrumgenomics.commons.util.StringUtil._
import com.fulcrumgenomics.commons.util.TimeUtil._
import dagr.core.tasksystem.Task
import dagr.core.tasksystem.Task.TaskInfo

import scala.collection.mutable.ListBuffer

/** Provides a method to provide an execution report for a task tracker */
trait FinalStatusReporter {

  /** The tasks on which we report. */
  def tasks: Traversable[Task]

  /** The header for the report */
  private def reportHeader: List[String] = List(
    "ID", "NAME", "STATUS", "CORES", "MEMORY",
    "SUBMISSION_DATE", "START_DATE", "END_DATE",
    "EXECUTION_TIME", "TOTAL_TIME",
    "SCRIPT", "LOG", "ATTEMPT_INDEX"
  )

  /** A row  for the report.  Each row is a given task. */
  private def reportRow(task: Task): List[String] = {
    val info = task.taskInfo
    val id: String = info.id.map(_.toString).getOrElse(task.name)
    reportRow(info=info, id=id, submissionDate=info.submissionDate, startDate=info.startDate, endDate=info.endDate)
  }

  /** A row  for the report.  Each row is a given task. */
  private def reportRow(info: TaskInfo,
                        id: String,
                        submissionDate: Option[Instant],
                        startDate: Option[Instant],
                        endDate: Option[Instant]): List[String] = {
    // get the total execution time, and total time since submission
    val (executionTime: String, totalTime: String) = info.executionAndTotalTime

    // The state of execution
    List(
      id,
      info.task.name,
      info.status.toString,
      f"${info.resources.map(_.cores.value).getOrElse(0.0)}%.2f",
      info.resources.map(_.memory.prettyString).getOrElse(""),
      timestampStringOrNA(submissionDate),
      timestampStringOrNA(startDate),
      timestampStringOrNA(endDate),
      executionTime,
      totalTime,
      info.script.map(_.toFile.getAbsolutePath).getOrElse(""),
      info.log.map(_.toFile.getAbsolutePath).getOrElse(""),
      info.attempts
    ).map(_.toString)
  }


  /** Writes a delimited string of the status of all tasks managed
    *
    * @param loggerMethod the method to use to write task status information, one line at a time
    * @param delimiter the delimiter between entries in a row
    */
  def logReport(loggerMethod: String => Unit, delimiter: String = "  "): Unit = {
    //val taskInfoMap: BiMap[Task, TaskExecutionInfo] = taskToInfoBiMapFor

    // Create the task status table
    val taskStatusTable: ListBuffer[List[String]] = new ListBuffer[List[String]]()
    taskStatusTable.append(reportHeader)
    val counter = new SimpleCounter[Task.TaskStatus]()
    // Go through every task
    tasks.toList.sortBy(task => (task.taskInfo.id.getOrElse(BigInt(-1)), task.name)).foreach { task =>
      val info = task.taskInfo
      // Make a report row
      taskStatusTable.append(reportRow(task))
      // Update the task status counts
      counter.count(info.status)
    }

    // Write the task status table
    loggerMethod(columnIt(taskStatusTable.toList, delimiter))

    // Create and write the task status counts
    val taskStatusCountTable = new ListBuffer[List[String]]()
    val keys: List[Task.TaskStatus] = counter.map(_._1).toList
    taskStatusCountTable.append(keys.map(_.toString))
    taskStatusCountTable.append(keys.map(status => counter.countOf(status).toString))
    loggerMethod("\n" + columnIt(taskStatusCountTable.toList, delimiter))
  }
}
