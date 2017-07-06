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
import java.time.Instant

import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.DagrDef._
import dagr.core.tasksystem.{InJvmTask, ProcessTask, UnitTask}

import scala.collection.mutable

/** A generic template for task execution managers. */
private[core] trait TaskExecutionRunnerApi {

  /** Start running a task.  Call [[TaskExecutionRunner.completedTasks]] to see if subsequently completes.
    *
    * @param taskInfo the info associated with this task.
    * @param simulate true if we are to simulate to run a task, false otherwise.
    * @return true if the task was started, false otherwise.
    */
  def runTask(taskInfo: TaskExecutionInfo, simulate: Boolean = false): Boolean


  // TODO: timeout should be an Option with type http://www.scala-lang.org/api/2.11.0/scala/concurrent/duration/Duration.html
  /** Get the completed tasks.
    *
    * @param failedAreCompleted true if treat tasks that fail as completed, false otherwise
    * @return a map from task identifiers to exit code and on complete success for all completed tasks.
    */
  def completedTasks(failedAreCompleted: Boolean = false): Map[TaskId, (Int, Boolean)]

  /** Get the running task identifiers.
    *
    * @return the set of task identifiers of running tasks.
    */
  def runningTaskIds: Iterable[TaskId]

  /** Get the running tasks.
    *
    * @return true if the task is running, false otherwise
    */
  def running(taskId: TaskId): Boolean

  // NB: does the underlying process.destroy work?
  /** Attempts to terminate a task's underlying process.
    *
    * @param taskId the identifier of the task to terminate
    * @return true if successful, false otherwise
    */
  def terminateTask(taskId: TaskId): Boolean

  /** Join on all the running processes until they are finished or the timeout reached. */
  def joinAll(millis: Long = 0) : Unit
}

private object TaskExecutionRunner {

  /** The exit code of any interrupted execution of a task. */
  val InterruptedExitCode = 255

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
  class ProcessTaskExecutionRunner(task: ProcessTask, script: Path, logFile: Path) extends TaskRunnable(task = task) {
    override def run(): Unit = {
      var process: Option[scala.sys.process.Process] = None
      try {
        val processBuilder: scala.sys.process.ProcessBuilder = task.processBuilder(script = script, logFile = logFile)
        process = Some(processBuilder.run())
        exitCode = process match {
          case Some(p) => p.exitValue()
          case None => 1
        }
      } catch {
        case e: InterruptedException =>
          this.exitCode = InterruptedExitCode
          this.throwable = Some(e)
        case t: Throwable =>
          this.exitCode = 1
          this.throwable = Some(t)
      }

      process.foreach(p => p.destroy())
      this.onCompleteSuccessful = Some(task.onComplete(this.exitCode))
    }
  }

  /** Simple class that runs the given task's method.
 *
    * @param task the task to run
    * @param script the location of the task's script
    * @param logFile the location of the task's log file
    */
  class InJvmTaskExecutionRunner(task: InJvmTask, script: Path, logFile: Path) extends TaskRunnable(task = task) {
    override def run(): Unit = {
      try {
        exitCode = task.inJvmMethod(script = script, logFile = logFile)
      } catch {
        case e: InterruptedException =>
          exitCode = InterruptedExitCode
          throwable = Some(e)
        case t: Throwable =>
          exitCode = 1
          throwable = Some(t)
      }
      onCompleteSuccessful = Some(task.onComplete(exitCode))
    }
  }

  /** Simple class to allow for simulation that sets the exit code to zero. */
  class SimulatedTaskExecutionRunner(task: UnitTask) extends TaskRunnable(task = task) {

    override def run(): Unit = exitCode = 0 // does absolutely nothing... well almost

    onCompleteSuccessful = Some(true)
  }
}

/** Tracks and run a set of [[dagr.core.tasksystem.Task]]s. */
private[core] class TaskExecutionRunner extends TaskExecutionRunnerApi with LazyLogging {
  import TaskExecutionRunner._
  private val processes: mutable.Map[TaskId, Thread] = new mutable.HashMap[TaskId, Thread]()
  private val taskRunners: mutable.Map[TaskId, TaskRunnable] = new mutable.HashMap[TaskId, TaskRunnable]()
  private val taskInfos: mutable.Map[TaskId, TaskExecutionInfo] = new mutable.HashMap[TaskId, TaskExecutionInfo]()

  /** Stop tracking the given task.
   *
   * @param taskId the task identifier
   * @return true if information was removed successfully, false otherwise.
   */
  protected def removeTask(taskId: TaskId): Boolean = {
    processes.remove(taskId).isDefined &&
      taskRunners.remove(taskId).isDefined &&
      taskInfos.remove(taskId).isDefined
  }

  /** Start running a task.  Call [[TaskExecutionRunner.completedTasks]] to see if it subsequently completes.
   *
   * @param taskInfo the info associated with this task.
   * @param simulate true if we are to simulate to run a task, false otherwise.
   * @return true if the task was started, false otherwise.
   */
  override def runTask(taskInfo: TaskExecutionInfo, simulate: Boolean = false): Boolean = taskInfo.task match {
    case unitTask: UnitTask =>
      try {
        unitTask.applyResources(taskInfo.resources.get)
        val taskRunner: TaskRunnable = (simulate, unitTask) match {
          case (true,  t: UnitTask)    => new SimulatedTaskExecutionRunner(task = t)
          case (false, t: InJvmTask)   => new InJvmTaskExecutionRunner(task = t, script = taskInfo.scriptPath.get, logFile = taskInfo.logPath.get)
          case (false, t: ProcessTask) => new ProcessTaskExecutionRunner(task = t, script = taskInfo.scriptPath.get, logFile = taskInfo.logPath.get)
          case _                       => throw new RuntimeException("Could not run a unknown type of task")
        }
        val thread = new Thread(taskRunner)
        processes.put(taskInfo.taskId, thread)
        taskRunners.put(taskInfo.taskId, taskRunner)
        taskInfos.put(taskInfo.taskId, taskInfo)
        thread.start()
        taskInfo.status = TaskStatus.Started
        taskInfo.startDate = Some(Instant.now())
        true
      }
      catch {
        case e: Exception =>
          logger.exception(e, s"Failed schedule for [${unitTask.name}]: ")
          taskInfo.status = TaskStatus.FailedScheduling
          false
      }
    case _ => throw new RuntimeException("Cannot call runTask on tasks that are not 'UnitTask's")
  }

  private def completeTask(taskId: TaskId,
                           taskInfo: TaskExecutionInfo,
                           exitCode: Int,
                           onCompleteSuccessful: Boolean,
                           throwable: Option[Throwable],
                           failedAreCompleted: Boolean = false): Unit = {


    // In case it has previously been stopped
    if (TaskStatus.notDone(taskInfo.status, failedIsDone=failedAreCompleted)) {
      taskInfo.endDate   = Some(Instant.now())
      taskInfo.status    = {
        if ((0 == exitCode && onCompleteSuccessful) || failedAreCompleted) TaskStatus.SucceededExecution
        else if (0 != exitCode) TaskStatus.FailedExecution
        else TaskStatus.FailedOnComplete // implied !onCompleteSuccessful
      }
      taskInfo.exitCode  = Some(exitCode)
      taskInfo.throwable = throwable
    }
    throwable.foreach { thr =>
        logger.error(
          s"task [${taskInfo.task.name}] had the following exception while executing: ${thr.getMessage}\n" +
            thr.getStackTrace.mkString("\n")
        )
    }
  }

  override def completedTasks(failedAreCompleted: Boolean = false): Map[TaskId, (Int, Boolean)] = {
    val completedTasks: mutable.Map[TaskId, (Int, Boolean)] = new mutable.HashMap[TaskId, (Int, Boolean)]()
    for ((taskId, thread) <- processes; if !thread.isAlive) {
      val taskRunnable = taskRunners(taskId)
      val exitCode: Int = taskRunnable.exitCode
      val onCompleteSuccessful: Boolean = taskRunnable.onCompleteSuccessful match {
        case Some(success) => success
        case None => throw new IllegalStateException(s"Could not find exit code for task with id '$taskId'")
      }
      val taskInfo = taskInfos(taskId)
      // update its end date, status, and log any exceptions
      completeTask(
        taskId               = taskId,
        taskInfo             = taskInfo,
        exitCode             = exitCode,
        onCompleteSuccessful = onCompleteSuccessful,
        throwable            = taskRunnable.throwable,
        failedAreCompleted   = failedAreCompleted
      )
      // store the relevant info in the completed tasks map
      completedTasks.put(taskId, (exitCode, onCompleteSuccessful))
      // we will no longer track this task
      removeTask(taskId)
    }
    completedTasks.toMap
  }

  override def running(taskId: TaskId): Boolean = taskInfos.contains(taskId)

  override def runningTaskIds: Iterable[TaskId] = {
    processes.keys
  }

  // NB: does the underlying process.destroy work?
  override def terminateTask(taskId: TaskId): Boolean = {
      processes.get(taskId) match {
        case Some(thread) =>
          val taskInfo = taskInfos(taskId)
          thread.join(1) // just give it 0.001 of second
          if (thread.isAlive) {
            // if it is alive, interrupt it
            thread.interrupt()
            thread.join(100) // just give it 0.1 of second
            taskInfo.status = TaskStatus.Stopped
          }
          taskInfo.endDate = Some(Instant.now())
          !thread.isAlive // thread is still alive WTF
        case _  => false
      }
  }

  /**
   * Join on all the running processes until they are finished or the timeout reached. A timeout value of
   * 0 means block indefinitely till the thread dies.
   */
  override def joinAll(millis: Long = 0): Unit = {
    if (millis == 0) {
      this.processes.values.foreach(_.join(millis))
    }
    else {
      val joinUntil = System.currentTimeMillis() + millis
      this.processes.values.foreach(p => {
        val timeout = joinUntil - System.currentTimeMillis()
        if (timeout > 0) p.join(timeout)
      })
    }
  }

  // for testing
  protected[execsystem] def onCompleteSuccessful(taskId: TaskId): Option[Boolean] = {
    taskRunners.get(taskId) match {
      case Some(thread) => thread.onCompleteSuccessful
      case None => None
    }
  }
}
