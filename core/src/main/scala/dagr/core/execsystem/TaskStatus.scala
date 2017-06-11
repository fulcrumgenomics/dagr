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

import dagr.core.execsystem.TaskStatus.{ManuallySucceeded, SucceededExecution}
import dagr.core.tasksystem.Task
import dagr.core.tasksystem.Task.{TaskStatus => RootTaskStatus}
import enumeratum.values.{IntEnum, IntEnumEntry}

sealed abstract class TaskStatus extends IntEnumEntry with Task.TaskStatus {
  override def ordinal = this.value
  /** Returns true if this status indicates any type of success, false otherwise. */
  def success: Boolean = this.ordinal == SucceededExecution.ordinal || this.ordinal == ManuallySucceeded.ordinal
}

case object TaskStatus extends IntEnum[TaskStatus] {
  val values = findValues
  
  /** Checks if a task with a given status is done.
    *
    * @param taskStatus the status of the task
    * @param failedIsDone true if we are to treat a failure as done
    * @return true if the task is done, false otherwise
    */
  def done(taskStatus: RootTaskStatus, failedIsDone: Boolean = true): Boolean = {
    val completed = taskStatus.isInstanceOf[Completed]
    if (failedIsDone) completed else !failed(taskStatus) && completed
  }

  /** Checks if a task with a given status is not done.
    *
    * @param taskStatus the status of the task
    * @param failedIsDone true if we are to treat failuer as done
    * @return true if the task is not done, false otherwise
    */
  def notDone(taskStatus: RootTaskStatus, failedIsDone: Boolean = true): Boolean = {
    !done(taskStatus, failedIsDone=failedIsDone)
  }

  /** Checks if a task with a given status is failed.
    *
    * @param taskStatus the status of the task
    * @return true if the task has failed, false otherwise
    */
  def failed(taskStatus: RootTaskStatus): Boolean = taskStatus.isInstanceOf[Failed]

  // Groups of statuses
  sealed trait Completed         extends TaskStatus
  sealed trait Failed            extends Completed
  sealed trait Succeeded         extends Completed

  // High-level statuses
  case object Unknown            extends TaskStatus { val description: String = "is unknown";                                              val value: Int = 0 }
  case object Started            extends TaskStatus { val description: String = "has been started";                                        val value: Int = 1 }
  case object Stopped            extends Completed  { val description: String = "has been stopped";                                        val value: Int = 2 }

  // Statuses after execution has completed
  case object FailedGetTasks     extends Failed     { val description: String = "has failed (could not get the list of tasks)";            val value: Int = 3 }
  case object FailedScheduling   extends Failed     { val description: String = "has failed (could not start executing after scheduling)"; val value: Int = 4 }
  case object FailedExecution    extends Failed     { val description: String = "has failed (execution)";                                  val value: Int = 5 }
  case object FailedOnComplete   extends Failed     { val description: String = "Failed during the onComplete callback";                   val value: Int = 6 }
  case object SucceededExecution extends Succeeded  { val description: String = "has succeeded";                                           val value: Int = 7 }
  case object ManuallySucceeded  extends Succeeded  { val description: String = "has succeeded (manually)";                                val value: Int = 8 }

  val TaskStatuses = Seq(Unknown, Started, Stopped, FailedGetTasks, FailedScheduling, FailedExecution, FailedOnComplete, SucceededExecution, ManuallySucceeded)
}
