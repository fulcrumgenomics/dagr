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

package dagr.api.models.tasksystem

/** Base class for all tasks, multi-tasks, and workflows.  A task executes a unit of work.  Itself may spawn other tasks,
  * or perform some atomic unit of work.  It may depend on other tasks, such that it should not be executed until the tasks
  * it depends on execute successfully (see [[Task.tasksDependedOn()]]).  Furthermore, it there may be other tasks
  * that depend on this task (see [[Task.tasksDependingOnThisTask()]]). */
trait Task[T] {

  /** The name of the task, by default the class's simple name. */
  var name: String = getClass.getSimpleName

  /** Sets the name of this task. */
  def withName(name: String) : this.type = { this.name = name; this }

  /** Gets the sequence of tasks that this task depends on.. */
  def tasksDependedOn: Traversable[Task[T]]

  /** Gets the sequence of tasks that depend on this task. */
  def tasksDependingOnThisTask: Traversable[Task[T]]

  /** Gets the execution information for the task, if available. For example, the information may not be available if
    * the task is not yet registered with the execution system. */
  def info: Option[TaskInfo[T]]
}
