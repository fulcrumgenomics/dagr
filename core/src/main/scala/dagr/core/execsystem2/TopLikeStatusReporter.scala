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

import java.io.ByteArrayOutputStream

import dagr.core.execsystem.SystemResources
import dagr.core.execsystem2.TaskStatus._
import dagr.core.reporting.{TopLikeStatusReporter => BaseTopLikeStatusReporter}
import dagr.core.tasksystem.Task

/** A simple top-like status reporter for [[dagr.core.execsystem2.GraphExecutor]].
  * @param systemResources the system resources used while executing
  * @param loggerOut the stream to which log messages are written, or none if no stream is available.
  * @param print the method to use to write task status information, one line at a time.
  */
class TopLikeStatusReporter(val systemResources: SystemResources,
                            protected val loggerOut: Option[ByteArrayOutputStream] = None,
                            protected val print: String => Unit = print)
  extends BaseTopLikeStatusReporter {


  /** True if the task is running, false otherwise. */
  protected[execsystem2] def running(task: Task): Boolean = task.taskInfo.status == Running

  /** True if the task is ready for execution (no dependencies), false otherwise. */
  protected[execsystem2] def queued(task: Task): Boolean = task.taskInfo.status == Queued || task.taskInfo.status == Submitted

  /** True if the task has failed, false otherwise. */
  protected[execsystem2] def failed(task: Task): Boolean = task.taskInfo.status.isInstanceOf[Failed]

  /** True if the task has succeeded, false otherwise. */
  protected[execsystem2] def succeeded(task: Task): Boolean = task.taskInfo.status == SucceededExecution

  /** True if the task has completed regardless of status, false otherwise. */
  protected[execsystem2] def completed(task: Task): Boolean = task.taskInfo.status.isInstanceOf[Completed]

  /** True if the task has unmet dependencies, false otherwise. */
  protected[execsystem2] def pending(task: Task): Boolean = task.taskInfo.status == Pending
}
