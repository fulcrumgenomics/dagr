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
import dagr.core.execsystem.{SystemResources, TaskManager}
import dagr.core.execsystem2.GraphExecutor
import dagr.core.execsystem2.local.LocalTaskExecutor
import dagr.core.reporting.ReportingDef.{TaskLogger, TaskRegister}
import dagr.core.reporting.{FinalStatusReporter, TaskStatusLogger}
import dagr.core.tasksystem.Task
import dagr.core.tasksystem.Task.{TaskInfo, TaskStatus}

import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext

object Executor {
  /** Create a new executor. */
  def apply(experimentalExecution: Boolean,
            resources: SystemResources,
            scriptsDirectory: Option[DirPath],
            logDirectory: Option[DirPath]
           )(implicit ex: ExecutionContext): Executor = {
    if (experimentalExecution) {
      GraphExecutor(new LocalTaskExecutor(systemResources=resources, scriptsDirectory=scriptsDirectory, logDirectory=logDirectory))
    }
    else {
      new TaskManager(taskManagerResources=resources, scriptsDirectory=scriptsDirectory, logDirectory=logDirectory)
    }
  }
}

/** All executors of tasks should extend this trait. */
trait Executor extends FinalStatusReporter {

  /** A list of [[TaskCache]] to use to determine if a task should be manually succeeded. */
  protected val taskCaches: ListBuffer[TaskCache] = ListBuffer[TaskCache]()

  /** The loggers to be notified when a task's status is updated. */
  private val _loggers: ListBuffer[TaskLogger] = ListBuffer[TaskLogger](new TaskStatusLogger)

  /** The loggers to be notified when a task's status is updated. */
  private val _registers: ListBuffer[TaskRegister] = ListBuffer[TaskRegister]()

  /** Record that the task information has changed for a task. */
  final def record(info: TaskInfo): Unit = this.synchronized {
    this._loggers.foreach(_.record(info=info))
  }

  /** The method that will be called on the result of `Task.getTasks`. */
  final def register(parent: Task, child: Task*): Unit = this.synchronized {
    this._registers.foreach(_.register(parent, child:_*))
  }

  /** Adds the [[dagr.core.reporting.ReportingDef.TaskLogger]] to the list of loggers to be notified when a task's status is updated. */
  final def withLogger(logger: TaskLogger): Unit = this.synchronized {
    if (!this._loggers.contains(logger)) {
      this._loggers.append(logger)
    }
  }

  /** Adds the [[TaskRegister]] to the list of registers to be notified when a list of tasks is returned by [[Task.getTasks]] */
  final def withTaskRegister(register: TaskRegister): this.type = this.synchronized {
    yieldAndThen[this.type](this)(this._registers.append(register))
  }

  /** Adds the [[TaskCache]] to the list of caches to use to determine if a task should be manually succeeded. */
  final def withTaskCache(taskCache: TaskCache): this.type = yieldAndThen[this.type](this) {
    this.withTaskRegister(taskCache)
    this.taskCaches.append(taskCache)
  }

  /** Start the execution of this task and all tasks that depend on it.  Returns the number of tasks that were not
    * executed.  A given task should only be attempted once. */
  final def execute(task: Task): Int = {
    task._executor = Some(this)
    this.register(task, task)
    this._execute(task=task)
  }

  /** Returns the task status by ordinal */
  def from(ordinal: Int): TaskStatus

  /** Returns the log directory. */
  def logDir: DirPath

  /** Start the execution of this task and all tasks that depend on it.  Returns the number of tasks that were not
    * executed.  A given task should only be attempted once.  All executors should implement this method. */
  protected def _execute(task: Task): Int
}
