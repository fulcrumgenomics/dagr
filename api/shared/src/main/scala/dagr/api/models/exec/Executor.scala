/*
 * The MIT License
 *
 * Copyright (c) 2017 Fulcrum Genomics
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

package dagr.api.models.exec

import dagr.api.models.tasksystem.{Task, TaskStatus}

/** Base class for all executors that execute tasks of type [[T]]. */
trait Executor[T<:Task[_]] {

  /** True if the task is running, false otherwise. */
  def running(task: T): Boolean

  /** True if the task is ready for execution (no dependencies), false otherwise. */
  def queued(task: T): Boolean

  /** True if the task has failed, false otherwise. */
  def failed(task: T): Boolean

  /** True if the task has succeeded, false otherwise. */
  def succeeded(task: T): Boolean

  /** True if the task has completed regardless of status, false otherwise. */
  def completed(task: T): Boolean

  /** True if the task has unmet dependencies, false otherwise. */
  def pending(task: T): Boolean

  /** Returns the task status by ordinal */
  def statusFrom(ordinal: Int): TaskStatus

  /** The list of statuses ordered by ordinal. */
  def statuses: Seq[TaskStatus]
}
