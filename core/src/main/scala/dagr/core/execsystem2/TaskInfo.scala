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

import java.time.Instant

import dagr.core.execsystem2.TaskStatus._
import dagr.core.tasksystem.Task

/** [[dagr.core.tasksystem.Task.TaskInfo]] implementation specific to [[dagr.core.execsystem2]]. */
class TaskInfo(task: Task, initStatus: TaskStatus)
  extends Task.TaskInfo(task=task, initStatus=initStatus) {

  /** Gets the instant that the task was submitted to the execution system. */
  override protected[core] def submissionDate: Option[Instant] = this.get(Pending)

  /** The instant the task started executing. */
  override protected[core] def startDate: Option[Instant]      = this.get(Running)

  /** The instant that the task finished executing. */
  override protected[core] def endDate: Option[Instant]        = latestStatus[Completed]
}
