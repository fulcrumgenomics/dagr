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

package dagr.core.execsystem2

import com.fulcrumgenomics.commons.CommonsDef.DirPath
import dagr.core.exec.SystemResources
import dagr.core.tasksystem.Task

import scala.concurrent.Future
import scala.concurrent.duration.Duration


/** An executor is responsible for executing one or more tasks.  That's it. How the the tasks are executed is
  * entirely up to the concrete implementations, including scheduling with resource management. This leaves A LOT
  * of things not specified for various executors (ex. SGE, PBS, local, mixed) on purpose. */
trait TaskExecutor[T<:Task] {
  /** simple name (not unique) for the executor. */
  def name: String = getClass.getSimpleName.replaceFirst("[$].*$", "")

  /** Execute a task. The first outer returned completes when a task is eligible for execution. It may delay, for
    * example, if there are not enough system resources to run.  The inner future completes when the task has completed
    * executing.  If the task can never by run, a failure should return immediately.
    *
    * A method `f` may be given, and will execute immediately prior to the task executing.
    */
  def execute(task: T, f: => Unit = () => Unit): Future[Future[T]]

  /** terminate a task, returns true if successful, false otherwise, None if it knows nothing about the task. */
  def kill(task: T, duration: Duration = Duration.Zero): Option[Boolean]

  /** true if the task is running, false if not running or not tracked */
  def running(task: T): Boolean

  /** true if the task executor knows about the task but has not run, false otherwise. */
  def waiting(task: T): Boolean

  /** true if the task executor knows about the task, false otherwise */
  def contains(task: T): Boolean

  /** The amount of resources this task executor can use to execute tasks. */
  def resources: SystemResources

  /** Returns the log directory. */
  def logDir: DirPath
}
