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

import dagr.core.DagrDef._
import dagr.core.execsystem.TaskManagerLike.BaseGraphNode
import dagr.core.tasksystem.Task
import dagr.commons.util.BiMap

private[execsystem] object TaskManagerLike {
  abstract class BaseGraphNode
}

/** A generic template for task managers */
private[execsystem] trait TaskManagerLike {

  /** Gets the task associated with the identifier, if any
    *
    * @param id the task id
    * @return the task associate with the id if found, None otherwise
    */
  def taskFor(id: TaskId): Option[Task]

  /** Get the task's execution information if it exists.
    *
    * @param id the task identifier.
    * @return the task execution information if the task is managed, None otherwise
    */
  def taskExecutionInfoFor(id: TaskId): Option[TaskExecutionInfo]

  /** Get the task's execution information if it exists.
    *
    * @param task the task.
    * @return the task execution information if the task is managed, None otherwise
    */
  def taskExecutionInfoFor(task: Task): Option[TaskExecutionInfo]

  /** Get the task's associated [[TaskStatus]].
    *
    * @param task the task.
    * @return the task status if the task is managed, None otherwise.
    */
  def taskStatusFor(task: Task): Option[TaskStatus.Value]


  /** Get the task's associated [[TaskStatus]].
    *
    * @param id the task identifier.
    * @return the task status if the task is managed, None otherwise.
    */
  def taskStatusFor(id: TaskId): Option[TaskStatus.Value]


  /** Get the task identifier for the given task.
    *
    * @param task the task.
    * @return the task identifier, None if the task is not being managed.
    */
  def taskFor(task: Task): Option[TaskId] = task._taskInfo.map(_.taskId)

  /** Get the task identifiers for all tracked tasks.
    *
    * @return the task ids
    */
  def taskIds(): Iterable[TaskId]

  /** Gets the graph execution node for the given id.  If there is no task being tracked
    * with that id, throws a [[NoSuchElementException]].
    *
    * @param id the id to lookup.
    * @return the associated task.
    */
  def apply(id: TaskId): BaseGraphNode

  /** Get the bi-directional map between managed tasks and their associated task execution information.
    *
    * @return the task and task execution information bi-directional map.
    */
  def taskToInfoBiMapFor: BiMap[Task, TaskExecutionInfo]

  /** Checks if we have failed tasks.
    *
    * @return true if we have failed tasks, false otherwise.
    */
  def hasFailedTasks: Boolean

  /** Adds a task to be managed
    *
    * @param task the given task.
    * @return the task identifier.
    */
  def addTask(task: Task): TaskId

  /** Adds tasks to be managed
    *
    * @param tasks the given tasks.
    * @return the task identifiers.
    */
  def addTasks(tasks: Task*): Seq[TaskId] = tasks map addTask

  /** Resubmit a task for execution.  This will stop the task if it is currently running, and queue
    * it up for execution.  The number of attempts will be reset to zero.
    *
    * @param task the task to resubmit.
    * @return true if the task was successfully resubmitted, false otherwise.
    */
  // Turning it off until `resubmit` is used.
  //def resubmitTask(task: Task): Boolean

  /** Replace the original task with the replacement task and update any internal references.
    *
    * If the replacement task depends on any task, any tasks depends on the replacement task, or if the original
    * task is not being tracked this method will return false.  Any tasks dependent on the original task, or any
    * task on which the original task depends, will have their dependency relationship updated.
    *
    * @param original the original task to replace.
    * @param replacement the replacement task.
    * @return true if we are successful, false otherwise.
    */
  def replaceTask(original: Task, replacement: Task): Boolean

  /** Run all tasks managed to either completion, failure, or inability to schedule.
    *
    * This will terminate tasks that were still running before returning in the case of failure or
    * inability to schedule.
    *
    * @param timeout           the length of time in milliseconds to wait for running tasks to complete
    * @return a bi-directional map from the set of tasks to their execution information.
    */
  def runToCompletion(timeout: Int = 1000): BiMap[Task, TaskExecutionInfo]

  /** Run a a single iteration of managing tasks.
    *
    * 1. Get tasks that have completed and update their state.
    * 2. Update any resolved dependencies in the execution graph.
    * 3. Get tasks for any task that has no predecessors until no more can be found.
    * 4. Schedule tasks and start running them.
    *
    * @param timeout the length of time in milliseconds to wait for running tasks to complete
    * @return a tuple of:
    *         (1) tasks that can be scheduled.
    *         (2) tasks that were scheduled.
    *         (3) tasks that are running prior to scheduling.
    *         (4) the tasks that have completed prior to scheduling.
    */
  def stepExecution(timeout: Int = 1000): (Traversable[Task], Traversable[Task], Traversable[Task], Traversable[Task])
}
