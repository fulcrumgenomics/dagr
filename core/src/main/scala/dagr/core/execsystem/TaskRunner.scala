/*
 * The MIT License
 *
 * Copyright (c) 2015-6 Fulcrum Genomics LLC
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
 */
package dagr.core.execsystem

import java.nio.file.Path
import java.sql.Timestamp

import dagr.core.tasksystem.{InJvmTask, ProcessTask, Task, UnitTask}
import dagr.core.util.LazyLogging

import scala.collection.mutable

private object TaskRunner {
  abstract class TaskRunnable(task: UnitTask) extends Runnable {
    var exitCode: Int = -1
    var onCompleteSuccessful: Option[Boolean] = None
    var throwable: Option[Throwable] = None
  }

  /** Simple class that runs the given task.  Wrap this in a thread,
    * and will set the exit code to one if the parent thread was interrupted,
    * otherwise the exit code will be set to that of the task's process.
    *
    * @param task the task to run
    * @param script the location of the task's script
    * @param logFile the location of the task's log file
    */
  class SingleTaskRunner(task: ProcessTask, script: Path, logFile: Path) extends TaskRunnable(task = task) {
    override def run(): Unit = {
      var process: Option[scala.sys.process.Process] = None
      try {
        val processBuilder: scala.sys.process.ProcessBuilder = task.getProcessBuilder(script = script, logFile = logFile)
        process = Some(processBuilder.run())
        exitCode = process.get.exitValue()
      } catch {
        case e: InterruptedException =>
          if (process.isDefined) process.get.destroy()
          exitCode = 1
          throwable = Some(e)
        case t: Throwable =>
          exitCode = 1
          throwable = Some(t)
      }
      onCompleteSuccessful = Some(task.onComplete(exitCode))
    }
  }

  /** Simple class that runs the given task's method.
    * @param task the task to run
    * @param script the location of the task's script
    * @param logFile the location of the task's log file
    */
  class InJvmTaskRunner(task: InJvmTask, script: Path, logFile: Path) extends TaskRunnable(task = task) {
    override def run(): Unit = {
      try {
        exitCode = task.inJvmMethod(script = script, logFile = logFile)
      } catch {
        case t: Throwable =>
          exitCode = 1
          throwable = Some(t)
      }
      onCompleteSuccessful = Some(task.onComplete(exitCode))
    }
  }

  /** Simple class that sets the exit code to zero */
  class NoOpTaskRunner(task: UnitTask) extends TaskRunnable(task = task) {

    override def run(): Unit = exitCode = 0 // does absolutely nothing... well almost

    onCompleteSuccessful = Some(true)
  }
}

/** Tracks and run a set of [[Task]]s. */
private[core] class TaskRunner extends LazyLogging {
  import TaskRunner._
  private val processes: mutable.Map[BigInt, Thread] = new mutable.HashMap[BigInt, Thread]()
  private val taskRunners: mutable.Map[BigInt, TaskRunnable] = new mutable.HashMap[BigInt, TaskRunnable]()
  private val taskInfos: mutable.Map[BigInt, TaskExecutionInfo] = new mutable.HashMap[BigInt, TaskExecutionInfo]()

  /** Stop tracking the given task.
   *
   * @param taskId the task identifier
   * @return true if information was removed successfully, false otherwise.
   */
  private def removeTask(taskId: BigInt): Boolean = {
    if (processes.remove(taskId).isEmpty) return false
    else if (taskRunners.remove(taskId).isEmpty) return false
    else if (taskInfos.remove(taskId).isEmpty) return false
    true
  }

  /** Start running a task.  Call [[TaskRunner.getCompletedTasks]] to see if subsequently completes.
   *
   * @param taskInfo the info associated with this task.
   * @param simulate true if we are to simulate to run a task, false otherwise.
   * @return true if the task was started, false otherwise.
   */
  def runTask(taskInfo: TaskExecutionInfo, simulate: Boolean = false): Boolean = taskInfo.task match {
    case unitTask: UnitTask =>
      try {
        unitTask.applyResources(taskInfo.resources)
        val taskRunner: TaskRunnable = (simulate, unitTask) match {
          case (true,  t: UnitTask)    => new NoOpTaskRunner(task = t)
          case (false, t: InJvmTask)   => new InJvmTaskRunner(task = t, script = taskInfo.script, logFile = taskInfo.logFile)
          case (false, t: ProcessTask) => new SingleTaskRunner(task = t, script = taskInfo.script, logFile = taskInfo.logFile)
          case _                       => throw new RuntimeException("Could not run a unknown type of task")
        }
        val thread = new Thread(taskRunner)
        processes.put(taskInfo.id, thread)
        taskRunners.put(taskInfo.id, taskRunner)
        taskInfos.put(taskInfo.id, taskInfo)
        thread.start()
        taskInfo.status = TaskStatus.STARTED
        taskInfo.startDate = Some(new Timestamp(System.currentTimeMillis))
        true
      }
      catch {
        case e: Exception =>
          logger.exception(e, s"Failed schedule for [${unitTask.name}]: ")
          taskInfo.status = TaskStatus.FAILED_SCHEDULING
          false
      }
    case _ => throw new RuntimeException("Cannot call runTask on tasks that are not 'UnitTask's")
  }

  /** Get the completed tasks.
   *
   * @param timeout the length of time in milliseconds to wait for running threads to join
   * @param failedAreCompleted true if treat tasks that fail as completed by setting their task status to [[TaskStatus.SUCCEEDED]], false otherwise
   * @return a map from task identifiers to exit code and on complete success for all completed tasks.
   */
  def getCompletedTasks(timeout: Int = 1000, failedAreCompleted: Boolean = false): Map[BigInt, (Int, Boolean)] = {
    val completedTasks: mutable.Map[BigInt, (Int, Boolean)] = new mutable.HashMap[BigInt, (Int, Boolean)]()
    for ((taskId, thread) <- processes) {
      thread.join(timeout)
      if (!thread.isAlive) {
        val exitCode: Int = taskRunners.get(taskId).get.exitCode
        val onCompleteSuccessful: Boolean = taskRunners.get(taskId).get.onCompleteSuccessful.get
        completedTasks.put(taskId, (exitCode, onCompleteSuccessful))
        val taskInfo = taskInfos.get(taskId).get
        taskInfo.endDate = Some(new Timestamp(System.currentTimeMillis))
        taskInfo.status = {
          if ((0 == exitCode && onCompleteSuccessful) || failedAreCompleted) TaskStatus.SUCCEEDED
          else if (0 != exitCode) TaskStatus.FAILED_COMMAND
          else TaskStatus.FAILED_ON_COMPLETE // implied !onCompleteSuccessful
        }
        if (taskRunners.get(taskId).get.throwable.isDefined) {
          val thr: Throwable = taskRunners.get(taskId).get.throwable.get
          logger.error(
            s"task [${taskInfo.task.name}] had the following exception while executing: " +
              thr.getMessage
              + "\n"
              + thr.getStackTrace.mkString("\n")
          )
        }
        removeTask(taskId)
      }
    }
    completedTasks.toMap
  }

  /** Get the running task identifiers.
   *
   * @return the set of task identifiers of running tasks.
   */
  def getRunningTasks: Iterable[BigInt] = {
    processes.keys
  }

  // NB: does the underlying process.destroy work?
  /** Attempts to terminate a task's underlying process.
   *
   * @param taskId the identifier of the task to terminate
   * @return true if successful, false otherwise
   */
  def terminateTask(taskId: BigInt): Boolean = {
    if (!processes.contains(taskId)) return false // not found
    val thread: Thread = processes.get(taskId).get
    thread.join(1) // just give it 0.001 of second
    if (thread.isAlive) { // if it is alive, interrupt it
      thread.interrupt()
      thread.join(100) // just give it 0.1 of second
    }
    val taskInfo = taskInfos.get(taskId).get
    taskInfo.endDate = Some(new Timestamp(System.currentTimeMillis))
    taskInfo.status = TaskStatus.FAILED_COMMAND
    if (thread.isAlive) return false // thread is still alive WTF
    true
  }

  // for testing
  private def getOnCompleteSuccessful(taskId: BigInt): Option[Boolean] = {
    taskRunners.get(taskId).get.onCompleteSuccessful
  }
}
