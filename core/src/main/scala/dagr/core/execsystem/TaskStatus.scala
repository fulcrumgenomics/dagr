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

import dagr.core.tasksystem.Task

/** The state for a [[Task]]. */
object TaskStatus extends Enumeration {
  type TaskStatus = Value
  val UNKNOWN, /** The task state is unknown, most likely not considered */
  STARTED, /** The task has been started */
  STOPPED, /* The task has been stopped */
  FAILED_GET_TASKS, /* The task could not get the list of tasks */
  FAILED_SCHEDULING, /* The task could not execute after scheduling */
  FAILED_COMMAND, /* The task command has failed */
  FAILED_ON_COMPLETE, /* The task has failed when trying to run its onComplete method */
  SUCCEEDED, /* The task has succeeded */
  MANUALLY_SUCCEEDED /* The task was manually succeeded */
  = Value

  /** Checks if a task with a given status is done.
    *
    * @param taskStatus the status of the task
    * @param failedIsDone true if we are to treat [[FAILED_COMMAND]] and [[FAILED_ON_COMPLETE]] and [[FAILED_SCHEDULING]] and [[FAILED_GET_TASKS]] as done
    * @return true if the task is done, false otherwise
    */
  def isTaskDone(taskStatus: TaskStatus, failedIsDone: Boolean = true): Boolean = {
    (isTaskFailed(taskStatus) && failedIsDone) || SUCCEEDED == taskStatus || MANUALLY_SUCCEEDED == taskStatus || STOPPED == taskStatus
  }

  /** Checks if a task with a given status is not done.
    *
    * @param taskStatus the status of the task
    * @param failedIsDone true if we are to treat [[FAILED_COMMAND]] and [[FAILED_ON_COMPLETE]] and [[FAILED_SCHEDULING]] and [[FAILED_GET_TASKS]] as done
    * @return true if the task is not done, false otherwise
    */
  def isTaskNotDone(taskStatus: TaskStatus, failedIsDone: Boolean = true): Boolean = {
    !isTaskDone(taskStatus, failedIsDone = failedIsDone)
  }

  def isTaskFailed(taskStatus: TaskStatus): Boolean = List(FAILED_COMMAND, FAILED_ON_COMPLETE, FAILED_SCHEDULING, FAILED_GET_TASKS).contains(taskStatus)
}
