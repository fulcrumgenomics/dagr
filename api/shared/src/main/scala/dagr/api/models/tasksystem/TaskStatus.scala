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

import dagr.api.models.DagrJsConversions
import upickle.Js
import upickle.default.{Reader, Writer}

object TaskStatus {
  import DagrJsConversions._

  implicit val taskStatusToWriter: Writer[TaskStatus] = Writer[TaskStatus] { status: TaskStatus =>fromTaskStatus(status) }
  implicit val taskStatusToReader: Reader[TaskStatus] = Reader[TaskStatus] { case obj: Js.Obj => toTaskStatus(obj) }
}

/** The status of a task.  Any execution system requiring a custom set of statuses should extend this trait. */
trait TaskStatus {
  /** A brief description of the status. */
  def description: String

  /** A unique ordinal for the status, used to prioritize reporting of statuses*/
  def ordinal: Int

  /** The name of the status, by default the class' simple name (sanitized). */
  def name: String = this.getClass.getSimpleName.replaceFirst("[$].*$", "")

  /** Returns true if this status indicates any type of success, false otherwise. */
  def success: Boolean

  /** Returns true if this status indicates any type of failure, false otherwise. */
  def failure: Boolean

  /** Returns true if this status indicates the task is executing, false otherwise. */
  def executing: Boolean

  /** The string representation of the status, by default the definition. */
  override def toString: String = this.description
}
