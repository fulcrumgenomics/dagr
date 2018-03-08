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

package dagr.core.exec

import com.fulcrumgenomics.commons.CommonsDef.{DirPath, yieldAndThen}
import com.fulcrumgenomics.commons.io.Io
import dagr.api.models.tasksystem.TaskStatus
import dagr.api.models.exec.{Executor => RootExecutor}
import dagr.core.execsystem.TaskManager
import dagr.core.execsystem2.{Executor => Executor2}
import dagr.core.execsystem2.local.LocalTaskExecutor
import dagr.core.reporting.ReportingDef.{TaskLogger, TaskRegister, TaskReporter}
import dagr.core.reporting.{FinalStatusReporter, TaskStatusLogger}
import dagr.core.tasksystem.Task
import dagr.core.tasksystem.Task.TaskInfo

import scala.collection.mutable.{Buffer, ListBuffer}
import scala.concurrent.ExecutionContext

object Executor {
  /** Create a new executor. */
  def apply(experimentalExecution: Boolean,
            resources: SystemResources,
            scriptDirectory: DirPath = Io.makeTempDir("scripts"),
            logDirectory: DirPath = Io.makeTempDir("logs"),
            failFast: Boolean = false
           )(implicit ex: ExecutionContext): Executor = {
    if (experimentalExecution) {
      Executor2(new LocalTaskExecutor(systemResources=resources, scriptDirectory=scriptDirectory, logDirectory=logDirectory))
    }
    else {
      new TaskManager(taskManagerResources=resources, scriptDirectory=scriptDirectory, logDirectory=logDirectory, failFast=failFast)
    }
  }
}

/** All executors of tasks should extend this trait.
  *
  * The executor is responsible for executing tasks in an order such that only tasks that do not depend on other tasks
  * are executed first.  When a task has no dependencies, it will be built, thereby allowing for any delayed configuration
  * (just-in-time) based on tasks on which it previously depended.  The task being built may either return itself
  * (fully-configured), in which case it is queued for execution, or may return one or more new tasks, in which case
  * those tasks are individually built and executed when they have no more unmet dependencies.  In the latter case, the
  * tasks that build other tasks are not executed per-se, but instead complete when all their children complete.
  *
  * There should be no cyclical dependencies, and any task should only be built by one other task (except for the root task
  * passed to the [[dagr.core.exec.Executor.execute(]] method who has no parent).  The latter implies that tasks that
  * build other tasks are in fact building distinct sub-trees of a dependency graph.
  *
  * The executor is also responsible for providing various types of event notifications:
  * 1. Notify that a task's status has changed (see [[dagr.core.exec.Executor.withLogger()]] and [[TaskLogger]]).
  * 2. Notify when a new task is built from another task via [[Task.getTasks()]] (see [[dagr.core.exec.Executor.withTaskRegister()]]
  *    and [[TaskRegister]]).
  *
  * Furthermore, an executor may choose to skip executing task (when it has no unmet dependencies) utilizing a [[TaskCache]],
  * typically from previous (failed) executions (see [[dagr.core.exec.Executor.withTaskCache()]].
  *
  * */
trait Executor extends FinalStatusReporter with RootExecutor[Task] {

  /** A list of [[TaskCache]] to use to determine if a task should be manually succeeded. */
  protected val taskCaches: Buffer[TaskCache] = ListBuffer[TaskCache]()

  /** The loggers to be notified when a task's status is updated. */
  private val _loggers: Buffer[TaskLogger] = ListBuffer[TaskLogger](new TaskStatusLogger)

  /** The registers to be notified when a task is built. */
  private val _registers: Buffer[TaskRegister] = ListBuffer[TaskRegister]()

  /** Record that the task information has changed for a task. */
  final def record(info: TaskInfo): Unit = this.synchronized {
    // Developer note: the task itself should call the record() method
    this._loggers.foreach(_.record(info=info))
  }

  /** Registers that the parent built the given children. */
  final def register(parent: Task, child: Task*): Unit = this.synchronized {
    // Developer note: the task itself should call the register() method
    this._registers.foreach(_.register(parent, child:_*))
  }

  /** Adds the [[dagr.core.reporting.ReportingDef.TaskLogger]] to the list of loggers to be notified when a task's status is updated. */
  private def withLogger(logger: TaskLogger): this.type = this.synchronized {
    if (!this._loggers.contains(logger)) {
      this._loggers.append(logger)
    }
    this
  }

  /** Adds the [[TaskRegister]] to the list of registers to be notified when a list of tasks is returned by [[Task.getTasks]] */
  private def withTaskRegister(register: TaskRegister): this.type = this.synchronized {
    yieldAndThen[this.type](this)(this._registers.append(register))
  }

  /** Adds the [[TaskCache]] to the list of caches to use to determine if a task should be manually succeeded. */
  private def withTaskCache(taskCache: TaskCache): this.type = yieldAndThen[this.type](this) {
    this.taskCaches.append(taskCache)
  }

  /** Adds teh [[TaskReporter]] to the list of reports to be notified depending on their type ([[TaskLogger]], [[TaskRegister]], or [[TaskCache]]). */
  final def withReporter(reporter: TaskReporter): this.type = this.synchronized {
    reporter match { case l: TaskLogger => withLogger(l); case _ => Unit }
    reporter match { case r: TaskRegister => withTaskRegister(r); case _ => Unit }
    reporter match { case c: TaskCache => withTaskCache(c); case _ => Unit }
    this
  }

  /** Start the execution of this task and all tasks that depend on it.  Returns the number of tasks that were not
    * executed.  A given task should only be attempted once. */
  final def execute(task: Task): Int = {
    task._executor = Some(this)
    this.register(task)
    this._execute(task=task)
  }

  /** Returns the task status by ordinal */
  def statusFrom(ordinal: Int): TaskStatus

  /** The list of statuses ordered by ordinal */
  def statuses: Seq[TaskStatus]

  /** Returns the log directory. */
  def logDir: DirPath

  /** Execute the given task and all tasks that depend on it.  Returns the number of tasks that were not
    * executed.  A given task should only be attempted once.  All executors should implement this method. */
  protected def _execute(task: Task): Int
}
