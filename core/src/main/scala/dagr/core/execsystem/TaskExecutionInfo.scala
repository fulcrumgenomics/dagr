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
package dagr.core.execsystem

import java.nio.file.Path
import java.time.{Duration, Instant}

import com.fulcrumgenomics.commons.util.TimeUtil._
import dagr.core.DagrDef._
import dagr.core.tasksystem.Task

/** The state of execution of a [[Task]].
 *
 * @param task the task that will be executed.
 * @param status the initial state of the task.
 * @param script the path to the script where the task commands should be stored.
 * @param logFile the path to the log file where the task stderr and stdout should be stored.
 * @param submissionDate the submission date of the task, if any.
 * @param resources the resources that the task was scheduled with.
 * @param startDate the start date of the task, if any.
 * @param endDate the end date of the task, if any.
 * @param attemptIndex the one-based count of attempts to run this task.
 */
class TaskExecutionInfo(var task: Task,
                        var taskId: TaskId,
                        var status: TaskStatus.Value,
                        var script: Path,
                        var logFile: Path,
                        var submissionDate: Option[Instant],
                        var resources: ResourceSet = ResourceSet(0,0),
                        var startDate: Option[Instant] = None,
                        var endDate: Option[Instant] = None,
                        var attemptIndex: Int = 1 // one-based
                         ) {
  if (attemptIndex < 1) throw new RuntimeException("attemptIndex must be greater than zero")

  task._taskInfo = Some(this)

  override def toString: String = {
    val na: String = "NA"
    s"STATUS[$status] ID[$taskId] NAME[${task.name}] SUBMITTED[${submissionDate.getOrElse(na)}]" +
    s" START[${startDate.getOrElse(na)}] END[${endDate.getOrElse(na)}] ATTEMPT[$attemptIndex]" +
    s" SCRIPT[$script] LOGFILE[$logFile]"
  }

  /** Gets the total execution time and total time since submission, in seconds, or None if the task has not started and ended.  Formats
    * the durations or return NA otherwise. */
  def durationSinceStartAndFormat: (String, String) = {
    durationSinceStart match {
      case Some((sinceStart, sinceSubmission)) => (formatElapsedTime(sinceStart), formatElapsedTime(sinceSubmission))
      case _ => ("NA", "NA")
    }
  }

  /** Gets the total execution time and total time since submission, in seconds, or None if the task has not started and ended. */
  private def durationSinceStart: Option[(Long, Long)] = (this.submissionDate, this.startDate, this.endDate) match {
    case (Some(submission), Some(start), Some(end)) =>
      val sinceSubmission = Duration.between(submission, end)
      val sinceStart      = Duration.between(start, end)
      Some(sinceStart.getSeconds, sinceSubmission.getSeconds)
    case _ => None
  }
}
