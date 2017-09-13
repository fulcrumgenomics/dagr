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

package dagr.core.reporting

import dagr.core.tasksystem.Task
import dagr.core.tasksystem.Task.TaskInfo

/** Contains base traits for classes that wish to be notified about task creation and status updates. */
object ReportingDef {

  /** Marker trait for all traits and classes that report information about a task. */
  trait TaskReporter

  /** Base trait for all classes interested in when the task status changes for any task. */
  trait TaskLogger extends TaskReporter {
    /** The method that will be called with updated task information. */
    def record(info: TaskInfo): Unit
  }

  /** Base trait for all classes interested in when a new task is built by another task (ex.
    * [[dagr.core.tasksystem.Pipeline]] */
  trait TaskRegister extends TaskReporter {
    /** The method that will be called on the result of `Task.getTasks`. */
    def register(parent: Task, child: Task*): Unit
  }
}
