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

package dagr.core.execsystem2.local

import com.fulcrumgenomics.commons.CommonsDef.{FilePath, yieldAndThen}
import dagr.core.execsystem2.util.InterruptableFuture
import dagr.core.execsystem2.util.InterruptableFuture.Interruptable
import dagr.core.tasksystem.{InJvmTask, ProcessTask, UnitTask}

import scala.concurrent._
import scala.concurrent.duration.Duration

/**
  * Trait that executes a single task, either in a shell ([[ProcessTask]]) or in the JVM ([[InJvmTask]]).
  *
  * The former is handled by [[dagr.core.execsystem2.local.LocalSingleTaskExecutor.ProcessSingleTaskExecutionExecutor]] and the latter
  * is handled by [[dagr.core.execsystem2.local.LocalSingleTaskExecutor.InJvmSingleTaskExecutionExecutor]].
  *
  * The local task runner is responsible for blocking on the execution (see the [[Future]] returned by the
  * [[dagr.core.execsystem2.local.LocalSingleTaskExecutor.execute()]] method), interrupting an executing task, and logging any
  * output.
  */
sealed trait LocalSingleTaskExecutor {
  private var interruptibleFuture: Option[InterruptableFuture[UnitTask]] = None

  /** The task to execute */
  def task: UnitTask

  /** The path to the script where the commands are stored. */
  def script: FilePath = task.taskInfo.script.getOrElse(throw new IllegalArgumentException(s"Task '${task.name}' does not have a script file"))

  /** THe path to the log file where logging information are stored. */
  def log: FilePath = task.taskInfo.log.getOrElse(throw new IllegalArgumentException(s"Task '${task.name}' does not have a log file"))

  /** The exit code from executing the task, if the task has completed, None otherwise. A non-zero exit code indicates
    * a failure. */
  def exitCode: Option[Int] = this.task.taskInfo.exitCode

  /** The throwable if thrown during execution, None otherwise.  A throwable indicates a failure. */
  def throwable: Option[Throwable] = this.task.taskInfo.throwable

  /** The method to execute the underlying task.  Completes when the underlying task completes. */
  final def execute()(implicit ex: ExecutionContext): Future[UnitTask] = {
    val future = blocking { _execute() } interruptable()
    this.interruptibleFuture = Some(future)
    future.future
  }

  /** Interrupts the execution of the task.  Returns the task if the task already completed, None otherwise. */
  def interrupt(): Option[UnitTask] = {
    updateExitCodeAndThrowable(code=Some(LocalSingleTaskExecutor.InterruptedExitCode))
    this.interruptibleFuture.flatMap(_.interrupt())
  }

  /** Returns true if the task was interrupted, false otherwise. */
  def interrupted(): Boolean = this.interruptibleFuture.exists(_.interrupted)

  /** Attempts to wait at most the given duration for the task to complete.  If the task does not complete, it is
    * interrupted.  Returns the task if the task completed successfully, None otherwise. */
  def join(atMost: Duration): Option[UnitTask] = {
    try {
      Some(Await.result(this.interruptibleFuture.get.future, atMost))
    } catch {
      case e: CancellationException => updateExitCodeAndThrowable(thr=Some(e)); interrupt()
      case e: TimeoutException      => updateExitCodeAndThrowable(thr=Some(e)); interrupt()
      case e: InterruptedException  => updateExitCodeAndThrowable(thr=Some(e)); interrupt()
    }
  }

  /** All sub-classes should implement this method to complete when the underlying task completes.  This will be called
    * by the [[execute()]] method. */
  protected def _execute()(implicit ex: ExecutionContext): Future[UnitTask]

  /** Updates the exit code and throwable upon completion of the task.  Only updates if the exit code or throwable are
    * defined respectively. */
  protected def updateExitCodeAndThrowable(code: Option[Int] = None,
                                           thr: Option[Throwable] = None): Unit = this.synchronized {
    val info = task.taskInfo
    code.foreach { c => info.exitCode = Some(c) }
    thr.foreach { t => info.throwable = Some(t) }
  }
}

object LocalSingleTaskExecutor {

  /** The exit code of any interrupted execution of a task. */
  val InterruptedExitCode = 255

  /** Creates a task runner for the given task.  Currently supports [[InJvmTask]]s or [[ProcessTask]]s only. */
  def apply(task: UnitTask): LocalSingleTaskExecutor = task match {
    case t: InJvmTask   => new InJvmSingleTaskExecutionExecutor(task=t)
    case t: ProcessTask => new ProcessSingleTaskExecutionExecutor(task=t)
    case _ => throw new RuntimeException(s"Cannot call execute on task '${task.name}' that are not 'UnitTask's.")
  }

  /** Simple class that runs the given task.  Wrap this in a thread,
    * and it will set the exit code to [[InterruptedExitCode]] if the parent thread was interrupted,
    * otherwise the exit code will be set to that of the task's process.
    *
    * @param task the task to run
    */
  class ProcessSingleTaskExecutionExecutor(val task: ProcessTask) extends LocalSingleTaskExecutor {
    def _execute()(implicit ex: ExecutionContext): Future[UnitTask] = Future {
      var process: Option[scala.sys.process.Process] = None
      try {
        val processBuilder: scala.sys.process.ProcessBuilder = task.processBuilder(script=script, logFile=log)
        process = Some(processBuilder.run())
        updateExitCodeAndThrowable(code=process.map(_.exitValue()).orElse(Some(1)))
      } catch {
        case e: InterruptedException =>
          updateExitCodeAndThrowable(code=Some(InterruptedExitCode), thr=Some(e))
        case t: Throwable =>
          updateExitCodeAndThrowable(code=Some(1), thr=Some(t))
      }

      process.foreach(p => p.destroy())
      task
    }
  }

  /** Simple class that runs the given task's method in the JVM.
    *
    * It will set the exit code to [[InterruptedExitCode]] if it was interrupted (via an [[InterruptedException]]), to
    * one if any other [[Throwable]] was encountered, or otherwise the value returned by the task's method.
    *
    * @param task the task to run
    */
  class InJvmSingleTaskExecutionExecutor(val task: InJvmTask) extends LocalSingleTaskExecutor {
    def _execute()(implicit ex: ExecutionContext): Future[UnitTask] = Future {
      try {
        val code = task.inJvmMethod(script=script, logFile=log)
        yieldAndThen(code)(updateExitCodeAndThrowable(code=Some(code)))
      } catch {
        case e: InterruptedException =>
          updateExitCodeAndThrowable(code=Some(InterruptedExitCode), thr=Some(e))
        case t: Throwable =>
          updateExitCodeAndThrowable(code=Some(1), thr=Some(t))
      }
      task
    }
  }
}
