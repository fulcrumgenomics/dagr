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

package dagr.api.models.tasksystem

import java.time.Instant

import dagr.api.DagrApi.TaskId
import dagr.api.models.util.{ResourceSet, TimePoint}

/** The execution information for a task.  Any execution system should extend this to store their task-specific metadata.*/
trait TaskInfo[T] {
  /** The task identifier. */
  def id         : Option[TaskId]

  /** The number of attempts at executing. */
  def attempts   : Int

  /** The path to the script (if any). */
  def script     : Option[T]

  /** The path to the log (if any). */
  def log        : Option[T]

  /** The amount of resources used by this task (if scheduled). */
  def resources  : Option[ResourceSet]

  /** The exit code (if completed). */
  def exitCode   : Option[Int]

  /** The throwable of the task (if failed with an exception). */
  def throwable  : Option[Throwable]

  /** The current status of the task. */
  def status     : TaskStatus

  /** The instant the task reached a given status. */
  def timePoints :  Traversable[TimePoint]

  /** The instant the task reached the current status. */
  def statusTime : Instant = timePoints.last.instant

  /** The name of the task. */
  def name: String

  /** Generates a human-readable string for use when loggin information about this task. */
  protected[dagr] def infoString: String = {
    val resourceMessage = this.resources match {
      case Some(r) => s"with ${r.cores} cores and ${r.memory} memory"
      case None    => ""
    }
    s"'$name' : ${this.status} on attempt #${this.attempts} $resourceMessage"
  }
}
