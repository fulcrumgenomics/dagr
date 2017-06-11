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

import java.nio.file.Path
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import com.fulcrumgenomics.commons.CommonsDef.DirPath
import com.fulcrumgenomics.commons.io.{Io, PathUtil}
import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.DagrDef.TaskId
import dagr.core.exec._
import dagr.core.execsystem._
import dagr.core.execsystem2.TaskExecutor
import dagr.core.execsystem2.util.InterruptableFuture
import dagr.core.execsystem2.util.InterruptableFuture.Interruptable
import dagr.core.tasksystem._

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration.Duration

/** Various defaults for executing locally.*/
object LocalTaskExecutorDefaults extends LazyLogging {

  /** The default resources to use when executing locally.  This includes both resources for [[InJvmTask]]s and
    * [[ProcessTask]]s (JVM and System resources respectively). */
  def defaultSystemResources: SystemResources = {
    val resources = SystemResources(cores=None, totalMemory=None) // Let the apply method figure it all out
    logger.debug("Defaulting System Resources to " + resources.cores.value + " cores and " + Resource.parseBytesToSize(resources.systemMemory.value) + " memory")
    logger.debug("Defaulting JVM Resources to " + Resource.parseBytesToSize(resources.jvmMemory.value) + " memory")
    resources
  }

  /** @return the default scheduler */
  def defaultScheduler: Scheduler = new NaiveScheduler
}

object LocalTaskExecutor {
  /** A class to facilitate scheduling and executing a task.
    *
    * @param taskRunner the task runner used to execute the task
    * @param latch the latch that will be zero when the task is scheduled to execute.
    * @param submitFuture the future that will complete once the task is scheduled.
    * @param executeFuture the future that will complete once the task has finished executing.  None if it has not
    *                      started executing.
    * @param resources the resources that the task was scheduled with, None if not scheduled.
    */
  protected case class TaskInfo
  (taskRunner: LocalTaskRunner,
   latch: CountDownLatch,
   submitFuture: InterruptableFuture[LocalTaskRunner],
   executeFuture: Option[InterruptableFuture[UnitTask]] = None,
   resources: Option[ResourceSet] = None
  )
}

/**
  *
  * @param systemResources the system (JVM and Process) resources to use
  * @param scriptsDirectory the directory to which to write script files (mainly for [[ProcessTask]]s).
  * @param logDirectory the directory to which to write log files (mainly for [[ProcessTask]]s).
  * @param scheduler the scheduler to use to allocate resources to tasks and decided in which order they should execute.
  * @param ex the execution context.
  */
class LocalTaskExecutor(systemResources: SystemResources = LocalTaskExecutorDefaults.defaultSystemResources,
                        scriptsDirectory: Option[Path] = None,
                        logDirectory: Option[Path] = None,
                        scheduler: Scheduler = LocalTaskExecutorDefaults.defaultScheduler
                       )(implicit ex: ExecutionContext) extends TaskExecutor[UnitTask] with LazyLogging {
  import LocalTaskExecutor.TaskInfo

  /** Provides a unique identifier for tasks. */
  private val nextTaskId = new AtomicInteger(1)

  /** The actual directory for script files. */
  private val actualScriptsDirectory = scriptsDirectory getOrElse Io.makeTempDir("scripts")

  /** The actual directory for log files. */
  private val actualLogsDirectory = logDirectory getOrElse Io.makeTempDir("logs")

  /** The map between the tasks and information about their scheduling and execution. */
  protected val taskInfo: mutable.Map[UnitTask, TaskInfo] = ExecDef.concurrentMap()

  logger.debug(s"Executing with ${systemResources.cores} cores and ${systemResources.systemMemory.prettyString} system memory.")
  logger.debug("Script files will be written to: " + actualScriptsDirectory)
  logger.debug("Logs will be written to: " + actualLogsDirectory)

  /** The system resources used by this executor */
  def resources: SystemResources = this.systemResources

  /** Returns the log directory. */
  override def logDir: DirPath = actualLogsDirectory

  /** Returns true if the task is running, false if not running or not tracked */
  def running(task: UnitTask): Boolean = {
    val info = this.taskInfo(task)
    info.resources.isDefined && info.executeFuture.isDefined
  }

  // true if the task has been scheduled for execution or is executing.
  //def scheduled(task: UnitTask): Boolean = this.taskInfo(task).resources.isDefined

  /** Returns true if the task has been submitted (i.e. is ready to execute) but has not been scheduled, false otherwise */
  def waiting(task: UnitTask): Boolean = this.taskInfo(task).resources.isEmpty

  /** Returns true if we know about this task, false otherwise */
  def contains(task: UnitTask): Boolean = this.taskInfo.contains(task)

  /** Kill the underlying task.  Returns None if the task is not being tracked.  Returns true if the task was explicitly
    * stopped.  Returns false if either if the task has not been started. or if the task could
    * not be stopped.
    */
  def kill(task: UnitTask, duration: Duration = Duration.Zero): Option[Boolean] = this.synchronized {
    this.taskInfo.get(task) match {
      case None => None
      case Some(info) =>
        val isRunning = running(task) // save this for later
        interrupt(future=info.submitFuture, duration=duration)
        info.executeFuture.foreach(f => interrupt(future=f, duration=duration))
        if (this.runningTasks.contains(task)) {
          this.schedule() // since the resources have been freed up
        }
        this.taskInfo.remove(task)
        Some(isRunning)
    }
  }

  /** Schedule and execute at task.  The outer future will complete once the task has been scheduled, while the inner
    * future will complete once the task has completed execution.
    */
  def execute(task: UnitTask): Future[Future[UnitTask]] = {
    throwableIfCanNeverBeScheduled(task) match {
      case Some(throwable) => Future.failed[Future[UnitTask]](throwable)
      case None =>
        // Submit it for scheduling
        val submitFuture = buildTaskInfoAndSubmitFuture(task=task)
        // Run the scheduler just in case it can be run immediately.
        this.schedule()
        // Execute it once it has been scheduled.
        submitFuture.future map { taskRunner => blocking { executeTaskRunner(taskRunner) } }
    }
  }

  /** Create the necessary information to schedule and execute a task, and returns a future that completes when
    * the task is scheduled for execution. */
  private def buildTaskInfoAndSubmitFuture(task: UnitTask): InterruptableFuture[LocalTaskRunner] = {
    require(task._taskInfo.isDefined, "Task._taskInfo is not defined")

    // Set some basic task info directly on the task
    val taskId = nextTaskId.getAndIncrement()
    task.taskInfo.id     = Some(taskId)
    task.taskInfo.script = Some(scriptPathFor(task, taskId, task.taskInfo.attempts))
    task.taskInfo.log    = Some(logPathFor(task, taskId, task.taskInfo.attempts))
    val taskRunner = LocalTaskRunner(task=task)

    // Create a future that waits until the task is scheduled for execution (the latch reaches zero).  It returusn the
    // task runner that executes the task.
    val latch = new CountDownLatch(1)
    val submitFuture = Future {
      blocking {
        latch.await()
      }
      taskRunner
    } interruptable()

    // Track the task.
    this.taskInfo.synchronized {
      this.taskInfo(task) = TaskInfo(
        taskRunner   = taskRunner,
        latch        = latch,
        submitFuture = submitFuture
      )
    }

    submitFuture
  }

  /** Run the task to completion and handle its completion. */
  private def executeTaskRunner(taskRunner: LocalTaskRunner): Future[UnitTask] = {
    val task = taskRunner.task
    // execute the task
    val executeFuture: InterruptableFuture[UnitTask] = taskRunner.execute() interruptable()
    // update the future that complets when the task execution complets
    this.taskInfo.synchronized {
      this.taskInfo.update(taskRunner.task, this.taskInfo(task).copy(executeFuture=Some(executeFuture)))
    }
    // update task information when it completes
    executeFuture map { t =>
      this.complete(t) // Regardless of success or failure, stop tracking the task
      this.schedule() // since maybe other tasks can now run with the freed up resources
      // Throw the throwable if it exists.
      taskRunner.throwable.foreach { thr =>
        throw new IllegalArgumentException(s"Task '${task.name}': ${thr.getMessage}", thr)
      }
      // Throw an exception if the task had a non-zero exit code, indicating failure
      if (!taskRunner.exitCode.contains(0)) throw new IllegalStateException(s"Task '${task.name}' had exit code '${taskRunner.exitCode.getOrElse("None")}'")
      task
    }
  }

  /** Gets the path to a task specific script or log file. */
  private def pathFor(task: UnitTask, taskId: TaskId, attemptIndex: Int, directory: Path, ext: String): Path = {
    val sanitizedName: String = PathUtil.sanitizeFileName(task.name)
    PathUtil.pathTo(directory.toString, s"$sanitizedName.$taskId.$attemptIndex.$ext")
  }

  /** Gets the path to a task specific script file. */
  private def scriptPathFor(task: UnitTask, taskId: TaskId, attemptIndex: Int): Path =
    pathFor(task, taskId, attemptIndex, actualScriptsDirectory, "sh")

  /** Gets the path to a task specific log file. */
  private def logPathFor(task: UnitTask, taskId: TaskId, attemptIndex: Int): Path =
    pathFor(task, taskId, attemptIndex, actualLogsDirectory, "log")

  /** Gets all tasks that have been submitted for execution but have not been scheduled. */
  private def toScheduleTasks: Set[UnitTask] = {
    this.taskInfo.flatMap { case (task, info) =>
      if (info.resources.isEmpty) Some(task) else None
    }.toSet
  }

  /** Gets a map from task to the resources it was scheduled with or None if not scheduled. */
  private def runningTasks: Map[UnitTask, ResourceSet] = this.taskInfo.flatMap { case (task, info) =>
    info.resources match {
      case Some(resources) => Some(task -> resources)
      case None            => None
    }
  }.toMap

  /** Waits for at most the given duration for the given future to complete, otherwise interrupting it. */
  private def interrupt[T](future: InterruptableFuture[T], duration: Duration = Duration.Zero): Unit = {
    try {
      Await.result(future.future, duration)
    } catch {
      case _: InterruptedException => future.interrupt()
      case _: TimeoutException     => future.interrupt()
    }
  }

  /** Completes the execution of this task, killing it if necessary.  The task will no longer be tracked. */
  private def complete(task: UnitTask): Boolean = kill(task=task) match {
    case None    => throw new IllegalArgumentException(s"Tried to remove a task that was not tracked: '${task.name}'")
    case Some(_) => true
  }

  /** Returns None if the task can be scheduled using all available system resources, otherwise, some exception that
    * can be thrown. */
  protected def throwableIfCanNeverBeScheduled(task: UnitTask): Option[Throwable] = {
    val canSchedule = scheduler.schedule(
      runningTasks = Map.empty,
      readyTasks   = Seq(task),
      systemCores  = systemResources.cores,
      systemMemory = systemResources.systemMemory,
      jvmMemory    = systemResources.jvmMemory
    ).nonEmpty

    if (canSchedule) None
    else {
      val msg = new StringBuilder
      msg.append(s"There will never be enough resources to schedule the task: '${task.name}'")

      // Attempt to estimate how much resources _it would take_ to execute the task.
      val resourcesType: String = task match {
        case _: FixedResources => "FixedResources"
        case _: VariableResources => "VariableResources"
        case _: Schedulable => "Schedulable"
        case _ => "Unknown Type"
      }
      val resources: Option[ResourceSet] = task match {
        case t: Schedulable => t.minResources(new ResourceSet(systemResources.cores, systemResources.systemMemory))
        case _ => None
      }
      val cores = resources.map(_.cores.toString).getOrElse("?")
      val memory = resources.map(_.memory.prettyString).getOrElse("?")
      msg.append(s"\nTask with name '${task.name}' requires $cores cores and $memory memory (task schedulable type: $resourcesType)")
      msg.append(s"\nThere are ${systemResources.cores} core(s) and ${systemResources.systemMemory.prettyString} system memory available.")

      Some(new IllegalArgumentException(msg.toString))
    }
  }

  /** Attempts to schedule tasks that at tracked but have not been scheduled. */
  private def schedule(): Boolean = this.synchronized {
    logger.debug("found " + toScheduleTasks.size + " ready tasks")
    this.toScheduleTasks.foreach { task =>
      logger.debug("ready task: " + task.name)
    }

    val tasksToSchedule: Map[UnitTask, ResourceSet] = scheduler.schedule(
      runningTasks = this.runningTasks,
      readyTasks   = this.toScheduleTasks,
      systemCores  = systemResources.cores,
      systemMemory = systemResources.systemMemory,
      jvmMemory    = systemResources.jvmMemory
    )

    tasksToSchedule.foreach { case (task, resources) =>
      task.applyResources(resources)
      val info = this.taskInfo(task)
      this.taskInfo.update(task, info.copy(resources=Some(resources)))
      info.latch.countDown() // release them from pre-submission
    }

    tasksToSchedule.nonEmpty
  }
}
