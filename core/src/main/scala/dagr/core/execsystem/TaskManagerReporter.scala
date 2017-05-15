/*
 * The MIT License
 *
 * Copyright (c) 2017 Fulcrum Genomics LLC
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

package dagr.core.execsystem

import dagr.core.execsystem.GraphNodeState.NO_PREDECESSORS
import dagr.core.reporting.TopLikeStatusReporter
import dagr.core.tasksystem.Task

/** A simple reporter for [[dagr.core.execsystem.TaskManager]]. */
class TaskManagerReporter(taskManager: TaskManager) extends TopLikeStatusReporter {

  /** the total system resources */
  protected def systemResources: SystemResources = taskManager.getTaskManagerResources

  /** the set of all tests about which are currently known */
  protected def tasks: Traversable[Task] = taskManager.taskToInfoBiMapFor.keys

  /** True if the task is running, false otherwise. */
  protected def running(task: Task): Boolean = task.taskInfo.status == TaskStatus.Started // taskManager.running(task)

  /** True if the task is ready for execution (no dependencies), false otherwise. */
  protected def queued(task: Task): Boolean = taskManager.graphNodeFor(task).get.state == GraphNodeState.NO_PREDECESSORS

  /** True if the task has failed, false otherwise. */
  protected def failed(task: Task): Boolean = TaskStatus.failed(task.taskInfo.status)

  /** True if the task has succeeded, false otherwise. */
  protected def succeeded(task: Task): Boolean = TaskStatus.done(task.taskInfo.status, failedIsDone=false)

  /** True if the task has completed regardless of status, false otherwise. */
  protected def completed(task: Task): Boolean = TaskStatus.done(task.taskInfo.status, failedIsDone=true)

  /** True if the task has unmet dependencies, false otherwise. */
  protected def pending(task: Task): Boolean = {
    val info = task.taskInfo
    info.status == TaskStatus.Unknown && taskManager.graphNodeStateFor(task).exists(_ != NO_PREDECESSORS)
  }
}