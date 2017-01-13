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
  val UNKNOWN            = Value("is unknown") /** The task state is unknown, most likely not considered */
  val STARTED            = Value("has been started") /** The task has been started */
  val STOPPED            = Value("has been stopped") /** The task has been stopped */
  val FAILED_GET_TASKS   = Value("has failed (could not get the list of tasks)") /** The task could not get the list of tasks */
  val FAILED_SCHEDULING  = Value("has failed (could not start executing after scheduling)") /** The task could not execute after scheduling */
  val FAILED_COMMAND     = Value("has failed (execution)") /** The task command has failed */
  val FAILED_ON_COMPLETE = Value("has failed (running the onComplete method)") /** The task has failed when trying to run its onComplete method */
  val SUCCEEDED          = Value("has succeeded") /** The task has succeeded */
  val MANUALLY_SUCCEEDED = Value("has succeeded (manually)") /**The task was manually succeeded */

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
