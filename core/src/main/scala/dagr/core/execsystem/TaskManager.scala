/*
 * The MIT License
 *
 * Copyright (c) 2015-2016 Fulcrum Genomics LLC
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
import java.time.{Duration, Instant}

import com.fulcrumgenomics.commons.collection.BiMap
import com.fulcrumgenomics.commons.io.{Io, PathUtil}
import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.DagrDef._
import dagr.core.execsystem
import dagr.core.tasksystem._

import scala.BigDecimal.RoundingMode.{HALF_UP => HalfUp}
import scala.annotation.tailrec

/** The resources needed for the task manager */
object SystemResources {
  /** Creates a new SystemResources that is a copy of an existing one. */
  def apply(that: SystemResources): SystemResources = {
    new SystemResources(cores = that.cores, systemMemory = that.systemMemory, jvmMemory = that.jvmMemory)
  }

  /** Creates a new SystemResources with the specified values. */
  def apply(cores: Double, systemMemory: Long, jvmMemory: Long): SystemResources = {
    new SystemResources(cores = Cores(cores), systemMemory = Memory(systemMemory), jvmMemory = Memory(jvmMemory))
  }

  /** Creates a new SystemResources with the cores provided and partitions the memory between system and JVM. */
  def apply(cores: Option[Cores] = None, totalMemory: Option[Memory] = None) : SystemResources = {
    val heapSize = Resource.heapSize

    val (system, jvm) = totalMemory match {
      case Some(memory) => (memory, heapSize)
      case None         => (Resource.systemMemory - heapSize, heapSize)
    }

    require(system.bytes > 0, "System memory cannot be <= 0 bytes.")

    new SystemResources(cores.getOrElse(Resource.systemCores), system, jvm)
  }

  val infinite: SystemResources = SystemResources(Double.MaxValue, Long.MaxValue, Long.MaxValue)
}

case class SystemResources(cores: Cores, systemMemory: Memory, jvmMemory: Memory)

/** Various defaults for task manager */
object TaskManagerDefaults extends LazyLogging {
  def defaultTaskManagerResources: SystemResources = {
    val resources = SystemResources(cores=None, totalMemory=None) // Let the apply method figure it all out
    val cores     = resources.cores.value
    val sysMemory = Resource.parseBytesToSize(resources.systemMemory.value)
    val jvmMemory = Resource.parseBytesToSize(resources.jvmMemory.value)
    logger.info(f"Defaulting System Resources to $cores%.2f cores and $sysMemory memory")
    logger.info(s"Defaulting JVM Resources to $jvmMemory memory")
    resources
  }

  /** @return the default scheduler */
  def defaultScheduler: Scheduler = new NaiveScheduler
}

/** Defaults and utility methods for a TaskManager. */
object TaskManager extends LazyLogging {
  import dagr.core.execsystem.TaskManagerDefaults._

  /** The initial time to wait between scheduling tasks. */
  val InitialSleepMillis: Int                    = 100
  /** The minimum time to wait between scheduling tasks. */
  val MinSleepMillis: Int                        = 10
  /** The maximum time to wait between scheduling tasks. */
  val MaxSleepMillis: Int                        = 1000
  /** The increased amount time to wait between scheduling tasks after nothing can be done (linear increase). */
  val StepSleepMillis: Int                       = 50
  /** The scaling factor to reduce (divide) the time by to wait between scheduling tasks (exponential backoff). */
  val BackoffSleepFactor: Float                  = 2f
  /** The maximum time between two attempts to task scheduling attempts after which a warning is logged. */
  val SlowStepTimeSeconds: Int                   = 30

  /** Runs a given task to either completion, failure, or inability to schedule.  This will terminate tasks that were still running before returning.
   *
   * @param task the task to run
   * @param taskManagerResources the set of task manager resources, otherwise we use the default
   * @param scriptsDirectory the scripts directory, otherwise we use the default
   * @param logDirectory the log directory, otherwise we use the default
   * @param scheduler the scheduler, otherwise we use the default
   * @param simulate true if we are to simulate running tasks, false otherwise
   * @return a bi-directional map from the set of tasks to their execution information.
   */
  def run(task: Task,
          taskManagerResources: Option[SystemResources] = Some(defaultTaskManagerResources),
          scriptsDirectory: Option[Path] = None,
          logDirectory: Option[Path] = None,
          scheduler: Option[Scheduler] = Some(defaultScheduler),
          simulate: Boolean = false,
          failFast: Boolean = false): BiMap[Task, TaskExecutionInfo] = {

    val taskManager: TaskManager = new TaskManager(
      taskManagerResources = taskManagerResources.getOrElse(defaultTaskManagerResources),
      scriptsDirectory     = scriptsDirectory,
      logDirectory         = logDirectory,
      scheduler            = scheduler.getOrElse(defaultScheduler),
      simulate             = simulate
    )

    taskManager.addTask(task = task)
    taskManager.runToCompletion(failFast=failFast)

    taskManager.taskToInfoBiMapFor
  }

}

/** A manager of tasks.
  *
  * No validation of whether or not we actually have the provided system or in-Jvm resources will occur.
  *
  * @param taskManagerResources the set of task manager resources, otherwise we use the default
  * @param scriptsDirectory     the scripts directory, otherwise a temporary directory will be used
  * @param logDirectory         the log directory, otherwise a temporary directory will be used
  * @param scheduler            the scheduler, otherwise we use the default
  * @param simulate             true if we are to simulate running tasks, false otherwise
  */
class TaskManager(taskManagerResources: SystemResources = TaskManagerDefaults.defaultTaskManagerResources,
                  scriptsDirectory: Option[Path]             = None,
                  logDirectory: Option[Path]                 = None,
                  scheduler: Scheduler                       = TaskManagerDefaults.defaultScheduler,
                  simulate: Boolean                          = false

) extends TaskManagerLike with TaskTracker with FinalStatusReporter with LazyLogging {

  private var curSleepMilliseconds: Int = TaskManager.InitialSleepMillis

  private val actualScriptsDirectory = scriptsDirectory getOrElse Io.makeTempDir("scripts")
  protected val actualLogsDirectory  = logDirectory getOrElse Io.makeTempDir("logs")

  private val taskExecutionRunner: TaskExecutionRunnerApi = new TaskExecutionRunner()

  import GraphNodeState._
  import TaskStatus._

  private val cores: Double  = taskManagerResources.cores.value
  private val memory: String = taskManagerResources.systemMemory.prettyString
  logger.info(f"Executing with $cores%.2f cores and $memory system memory.")
  logger.info("Script files will be written to: " + actualScriptsDirectory)
  logger.info("Logs will be written to: " + actualLogsDirectory)

  private[core] def getTaskManagerResources: SystemResources = taskManagerResources

  private def pathFor(task: Task, taskId: TaskId, attemptIndex: Int, directory: Path, ext: String): Path = {
    val sanitizedName: String = PathUtil.sanitizeFileName(task.name)
    PathUtil.pathTo(directory.toString, s"$sanitizedName.$taskId.$attemptIndex.$ext")
  }

  override protected def scriptPathFor(task: Task, taskId: TaskId, attemptIndex: Int): Path = pathFor(task, taskId, attemptIndex, actualScriptsDirectory, "sh")
  override protected def logPathFor(task: Task, taskId: TaskId, attemptIndex: Int): Path = pathFor(task, taskId, attemptIndex, actualLogsDirectory, "log")

  /** Attempts to terminate a task's underlying process.
    *
    * @param taskId the identifier of the task to terminate
    * @return true if successful, false otherwise
    */
  protected def terminateTask(taskId: TaskId): Boolean = taskExecutionRunner.terminateTask(taskId=taskId)

  /** For testing purposes only. */
  protected def joinOnRunningTasks(millis: Long) = taskExecutionRunner.joinAll(millis)

  /** For testing purposes only. */
  private[execsystem] def completedTasks(failedAreCompleted: Boolean): Map[TaskId, (Int, Boolean)] = taskExecutionRunner.completedTasks(failedAreCompleted=failedAreCompleted)

  /** Logs a message for the given task. */
  private def logTaskMessage(taskInfo: TaskExecutionInfo): Unit = {
    val cores  = taskInfo.resources.cores.value
    val memory = taskInfo.resources.memory.prettyString
    logger.info(f"'${taskInfo.task.name}' ${taskInfo.status} on attempt #${taskInfo.attemptIndex} with $cores%.2f cores and $memory memory")
  }

  /** Replace the original task with the replacement task and update
   * any internal references.  This will terminate the task if it is running.
   *
   * @param original the original task to replace.
   * @param replacement the replacement task.
   * @return true if we are successful, false if the original is not being tracked.
   */
  override def replaceTask(original: Task, replacement: Task): Boolean = {
    taskFor(original) match {
      case Some(taskId) =>
        val taskInfo = original.taskInfo

        if (taskInfo.status == STARTED) {
          // the task has been started, kill it
          terminateTask(taskId)
          processCompletedTask(taskId = taskId, doRetry = false)
        }

        super.replaceTask(original = original, replacement = replacement)
        true
      case None => false
    }
  }

  // This method fails occasionally, so there likely is a race condition.  Turning it off until `resubmit` is used.
  /*
  override def resubmitTask(task: Task): Boolean = {
    taskIdFor(task) match {
      case Some(taskId) =>
        val infoAndNode = infoFor(task)
        val taskInfo    = infoAndNode.info
        val node        = infoAndNode.node

        // check if the task is running and if so, kill it
        if (taskInfo.status == STARTED) terminateTask(taskId)

        // update all complete tasks, as this task may have already completed, or was just terminated
        updateCompletedTasks()

        // reset the internal data structures for this task
        taskInfo.status = TaskStatus.UNKNOWN
        taskInfo.attemptIndex = 1
        node.state = NO_PREDECESSORS

        true
      case _ =>
        false
    }
  }
  */

  /** Compares two timestamp options.  If either option is empty, zero is returned. */
  private def compareOptionalInstants(left: Option[Instant], right: Option[Instant]): Int = (left, right) match {
    case (Some(l), Some(r)) => l.compareTo(r)
    case _ => 0
  }

  /** Updates the start and end date for a parent, if it exists.  If the task failed, then also set the parent status
    * to failed */
  private def updateParentStartAndEndDates(node: GraphNode, info: Option[TaskExecutionInfo] = None): Unit = {
    // Set the start and end dates for the parent, if they exist
    node.enclosingNode.foreach { parent =>
      logger.debug(s"updateParentStartAndEndDates: updating parent [${parent.task.name}] [${parent.task.tasksDependingOnThisTask.size}]")
      val parentTaskInfo = parent.taskInfo
      val taskInfo: TaskExecutionInfo = info.getOrElse(node.taskInfo)
      // start date
      if (compareOptionalInstants(parentTaskInfo.startDate, taskInfo.startDate) >= 0) {
        parentTaskInfo.startDate = taskInfo.startDate
      }
      // end date
      if (compareOptionalInstants(parentTaskInfo.endDate, taskInfo.endDate) <= 0) {
        parentTaskInfo.endDate = taskInfo.endDate
      }
      // status if the child task failed
      if (TaskStatus.isTaskFailed(taskInfo.status) && !TaskStatus.isTaskFailed(parentTaskInfo.status)) {
        assert(parentTaskInfo.status == TaskStatus.STARTED)
        parentTaskInfo.status = taskInfo.status
        // Update the parent's parent(s), if any
        updateParentStartAndEndDates(node=parent, info=Some(parentTaskInfo))
      }
      logger.debug(s"updateParentStartAndEndDates: updating parent [${parent.task.name}] [${parent.task.tasksDependingOnThisTask.size}]")
    }
  }

  /** Sets the state of the node to completed, and updates the start and end date of any parent */
  private def completeGraphNode(node: GraphNode,  info: Option[TaskExecutionInfo] = None): Unit = {
    updateParentStartAndEndDates(node, info)
    node.state = GraphNodeState.COMPLETED
  }

  /** Update internal data structures for the completed task.  If a task has failed,
   * retry it.
   *
   * @param taskId the task identifier for the completed task.
   */
  private[execsystem] def processCompletedTask(taskId: TaskId, doRetry: Boolean = true): Unit = {
    val node      = this(taskId)
    val taskInfo  = node.taskInfo
    logTaskMessage(taskInfo=taskInfo)
    val updateNodeToCompleted: Boolean = if (TaskStatus.isTaskFailed(taskStatus = taskInfo.status) && doRetry) {
      val retryTask = taskInfo.task match {
        case retry: Retry => retry.retry(this.getTaskManagerResources, taskInfo)
        case _ => false
      }
      if (retryTask) {
        // retry it
        logger.debug("task [" + taskInfo.task.name + "] is being retried")
        node.state = NO_PREDECESSORS
        taskInfo.attemptIndex += 1
        taskInfo.script  = scriptPathFor(task=taskInfo.task, taskId=taskInfo.taskId, attemptIndex=taskInfo.attemptIndex)
        taskInfo.logFile = logPathFor(task=taskInfo.task, taskId=taskInfo.taskId, attemptIndex=taskInfo.attemptIndex)
        false // do not update the node to completed
      }
      else {
          logger.debug("task [" + taskInfo.task.name + "] is *not* being retried")
          true
      }
    }
    else {
      true
    }
    if (updateNodeToCompleted) {
      logger.debug("processCompletedTask: Task [" + taskInfo.task.name + "] updating node to completed")
      if (TaskStatus.isTaskNotDone(taskInfo.status, failedIsDone = true)) {
        if (taskInfo.task.isInstanceOf[UnitTask]) {
          throw new RuntimeException(s"Processing a completed UnitTask but it was not done! status: ${taskInfo.status}")
        }
        // Set it to succeeded as it no longer has predecessors
        taskInfo.status = TaskStatus.SUCCEEDED
      }
      completeGraphNode(node, Some(taskInfo))
    }
  }

  /** Gets the set of newly completed tasks and updates their state (to either retry or complete)
    *
    * @return for each completed task, a map from a task identifier to a tuple of the exit code and the status of the `onComplete` method
    */
  private def updateCompletedTasks(): Seq[Task] = {
    // Get the tasks that are not eligible to be scheduled/executed, and have no predecessors (can be completed).
    // Developer note:
    // A task that returns one or more "other" tasks in getTasks, and so are not executed by the `taskExecutionRunner`,
    // are included only if the tasks returned are themselves completed.  This is handed in `getTasks`, where we updated
    // the predecessors of a task to include child tasks returned by the parent's `getTasks` call.  The child will be
    // removed as a predecessor when the child completes successfully. Therefore, the parent task will only have state
    // `NO_PREDECESSORS` if all child tasks have `SUCCEEDED`.  See the following methods:
    // - `updatePredecessors`: removes the child as a predecessor of the parent when the child succeeds
    // - `updateParentStartAndEndDates`: updates the parent start/end execution date based on the children's start/end date
    val toCompleteTasks: Seq[Task] = graphNodesInStateFor(GraphNodeState.NO_PREDECESSORS)
      .filterNot(_.task.isInstanceOf[UnitTask])
      .map(_.task)
      .toIndexedSeq
    // Get the tasks completed by the task executor
    val completedTasks: Seq[Task] = taskExecutionRunner.completedTasks().keys.map(this.taskFor(_).get).toIndexedSeq

    // Set them to complete
    (toCompleteTasks ++ completedTasks).foreach { task =>
      processCompletedTask(task.taskInfo.taskId)
      logger.debug(f"updateCompletedTasks: unit-task [${task.name}] completed with task status [${task.taskInfo.status}]")
    }

    completedTasks
  }

  // Update orphans: tasks whose predecessors have not been added to the scheduler.  These nodes may go into
  // the PREDECESSORS_AND_UNEXPANDED state, so try it before removing completed predecessors.
  /** For all tasks whose predecessors who have not previously been added to this manager (predecessors are defined as tasks on
    * which they depend), who should be in the [[ORPHAN]] state, check if all their predecessors are now tracked.  If so,
    * move them to the [[PREDECESSORS_AND_UNEXPANDED]] state.  We are unable to process any task has tasks we do not know
    * about on which it is dependent (i.e. the task has unknown predecessors).
    */
  private def updateOrphans(): Unit = {
    // find nodes where all predecessors have an associated graph node
    graphNodesInStateFor(ORPHAN).filter(node => allPredecessorsAdded(task = node.task)).foreach(node => {
      // add the predecessors
      logger.debug("updateOrphans: found an orphan task [" + node.task.name + "] that has [" +
        predecessorsOf(task=node.task).getOrElse(Nil).size + "] predecessors")
      node.addPredecessors(predecessorsOf(task=node.task).get)
      logger.debug("updateOrphans: orphan task [" + node.task.name + "] now has [" + node.numPredecessors + "] predecessors")
      // update its state
      node.state = PREDECESSORS_AND_UNEXPANDED
    })
  }

  /**  Remove predecessors in the execution graph for any completed predecessor.
    *
    *  For each execution node in our graph, find those with predecessors.  For each predecessor, remove it if that
    *  task has completed.  For tasks that are not [[UnitTask]], we can immediately set them to [[SUCCEEDED]] since
    *  they do not actually perform a task, but simply generate a list of tasks themselves.  Otherwise, set the task
    *  to have no predecessors: [[NO_PREDECESSORS]].  If we find the former case, we need to perform this procedure again,
    *  since some tasks go strait to succeeded and we may have successor tasks (children) that can now execute.
    */
  @tailrec
  private def updatePredecessors(): Unit = {
    var hasMore = false
    graphNodesWithPredecessors.foreach { node =>
      node.predecessors
        .filter(p => p.state == GraphNodeState.COMPLETED && TaskStatus.isTaskDone(p.taskInfo.status, failedIsDone=false))
        .foreach(node.removePredecessor)
      //logger.debug("updatePredecessors: examining task [" + node.task.name + "] for predecessors: " + node.hasPredecessor)
      // - if this node has already been expanded and now has no predecessors, then move it to the next state.
      // - if it hasn't been expanded and now has no predecessors, it should get expanded later
      //if (!node.hasPredecessor) logger.debug(s"updatePredecessors: has node state: ${node.state}")
      if (!node.hasPredecessor && node.state == ONLY_PREDECESSORS) {
        val taskInfo = node.taskInfo
        node.task match {
          case _: UnitTask => node.state = NO_PREDECESSORS
          case _ =>
            completeGraphNode(node)
            taskInfo.status = TaskStatus.SUCCEEDED
            hasMore = true // try again for all successors, since we have more nodes that have completed
        }
        //logger.debug(s"updatePredecessors: task [${node.task.name}] now has node state [${node.state}] and status [${taskInfo.status}]")
      }
    }

    // If we moved a task straight to the completed state, do this procedure again, as we may have tasks that depend
    // on the former task that can execute because the former task is their last dependency.
    if (hasMore) updatePredecessors()
  }

  /** Invokes `getTasks` on the task associated with the graph node.
    *
    * (1) In the case that `getTasks` returns the same exact task, it verifies it is a [[UnitTask]].  Since
    * the task already has an execution node, it must have already passed to [[addTask()]].
    *
    * (2) In the case that `getTasks` returns a different task, or more than one task, set the submission date of the node,
    * set the submission date, and add each task to the task manager's execution graph.  We make the original task on
    * which we called `getTasks` dependent on the task(s) returned by `getTasks`.
    *
    * Returns true if we found new tasks (2), false otherwise (1).
    * */
  private def invokeGetTasks(node: GraphNode): Boolean = {

    // get the list of tasks that this task generates
    val tasks = try { node.task.getTasks.toSeq } catch { case e: Exception => throw new TaskException(e, TaskStatus.FAILED_GET_TASKS) }
    // NB: we don't create a new node for this task if it just returns itself
    // NB: always check for cycles, since we don't know when they could be introduced.  We will check
    //     for cycles in [[addTask]] so only check here if [[getTasks]] returns itself.
    tasks match {
      case Nil => // no tasks returned
        logger.debug(f"invokeGetTasks 1 ${node.task.name} : ${tasks.map(_.name).mkString(", ")}")
        // set the submission time stamp
        node.taskInfo.submissionDate = Some(Instant.now())
        false
      case x :: Nil if x == node.task => // one task and it returned itself
        logger.debug(f"invokeGetTasks 2 ${node.task.name} : ${tasks.map(_.name).mkString(", ")}")
        // Developer note: removing the check for cycles here because we should have checked for cycles when the task
        // was added!
        // check for cycles only when we have a unit task for which calling [[getTasks] returns itself.
        // checkForCycles(task = node.task)
        // verify we have a UnitTask
        node.task match {
          case _: UnitTask =>
          case _ => throw new RuntimeException(s"Task was not a UnitTask!")
        }
        false
      case _ =>
        logger.debug(f"invokeGetTasks 3 ${node.task.name} : ${tasks.map(_.name).mkString(", ")}")
        // set the submission time stamp
        node.taskInfo.submissionDate = Some(Instant.now())
        // we will make this task dependent on the tasks it creates...
        if (tasks.contains(node.task)) throw new IllegalStateException(s"Task [${node.task.name}] contained itself in the list returned by getTasks")
        // track the new tasks. If they are already added, that's fine too.
        val taskIds: Seq[TaskId] = addTasks(tasks, enclosingNode = Some(node), ignoreExists = true)
        // make this node dependent on those tasks
        taskIds.map(taskId => node.addPredecessors(this(taskId)))
        // we may need to update predecessors if a returned task was already completed
        if (tasks.flatMap(t => graphNodeFor(t)).exists(_.state == GraphNodeState.COMPLETED)) updatePredecessors()
        // TODO: we could check each new task to see if they are in the PREDECESSORS_AND_UNEXPANDED state
        true
    }
  }

  /** Updates the node's state and associated task's status.
    *
    * If the task is a [[UnitTask]] it should have no predecessors.
    *
    * If the task is not a [[UnitTask]] we have just added the tasks returned by its `getTasks` method, so
    * set the task dependent on those returned tasks, and update its status to started.
    * */
  private def updateNodeStateAndTasksStatusAfterGetTasks(node: GraphNode): Unit = {
    node.task match {
      case _: UnitTask => node.state = NO_PREDECESSORS
      case _ =>
        val taskInfo = node.taskInfo
        // In the case that this node is not a [[UnitTask]] we should wait until all of the tasks returned by
        // its [[getTasks]] method have completed, so set it to have only predecessors in that case.
        if (node.hasPredecessor) {
          node.state = GraphNodeState.ONLY_PREDECESSORS
          taskInfo.status = TaskStatus.STARTED
        }
        else { // if `getTasks` returned no tasks, then just update it to succeeded
          node.state = NO_PREDECESSORS
          taskInfo.status = TaskStatus.STARTED
        }
    }
  }

  /** For all unexpanded tasks with no predecessors, invoke any callbacks (`invokeCallbacks`) and get any tasks
    * they generate (`getTasks`).  This will add those tasks to the task manager.
    *
    */
  private def invokeCallbacksAndGetTasks(): Unit = {
    var hasMore = false
    // find all tasks that are unexpanded and previously have dependencies (predecessors) but no longer do.
    for (node <- graphNodesInStateFor(PREDECESSORS_AND_UNEXPANDED).filterNot(_.hasPredecessor)) {
      logger.debug("invokeCallbacksAndGetTasks: found node in state PREDECESSORS_AND_UNEXPANDED with no predecessors: task [" + node.task.name + "]")
      try {
        // call get tasks
        if (invokeGetTasks(node)) hasMore = true
        // update the execution node's state and task status
        updateNodeStateAndTasksStatusAfterGetTasks(node)
      }
      catch {
        // Catch any exception so we can just fail this task.
        case e: Exception =>
          val taskInfo: TaskExecutionInfo = node.taskInfo
          completeGraphNode(node, Some(taskInfo))
          taskInfo.status = e match {
            case ex: TaskException => ex.status
            case _ => FAILED_GET_TASKS
          }
          logger.exception(e, s"Failed getTasks for [${node.task.name}]: ")
      }
    }
    // update predecessors just in case some tasks added were already completed
    updatePredecessors()
    // if we added more tasks of non-[[UnitTask]] type, then we should check again
    if (hasMore) invokeCallbacksAndGetTasks()
  }

  /** For each task and resource set, add that task to the task runner for execution allocating the given
    * resources.
    *
    * @param tasksToSchedule a map from a given task to execute to the resources it has been allocated.
    */
  private def scheduleAndRunTasks(tasksToSchedule: Map[UnitTask, ResourceSet]): Unit = {
    // schedule them and update the node status
    for ((task, taskResourceSet) <- tasksToSchedule) {
      val taskInfo = task.taskInfo
      val node     = this(taskInfo.taskId)
      taskInfo.resources = taskResourceSet // update the resource set that was used when scheduling the task
      if (taskExecutionRunner.runTask(taskInfo, simulate)) {
        node.state = RUNNING
        logTaskMessage( taskInfo=taskInfo)
      }
      else {
        completeGraphNode(node, Some(taskInfo))
        logTaskMessage(taskInfo=taskInfo)
      }
    }
  }

  /** Returns a map of running tasks and the resource set they were scheduled with. */
  def runningTasksMap: Map[UnitTask, ResourceSet] = Map(taskExecutionRunner.runningTaskIds.toList.map(id => this(id).task).map(task => (task.asInstanceOf[UnitTask], task.taskInfo.resources)) : _*)

  protected[core] def readyTasksList: List[UnitTask] = graphNodesInStateFor(NO_PREDECESSORS).toList.map(node => node.task.asInstanceOf[UnitTask])

  override def stepExecution(): (Iterable[Task], Iterable[Task], Iterable[Task], Iterable[Task]) = {
    logger.debug("stepExecution: starting one round of execution")

    // get newly completed tasks
    val completedTasks = updateCompletedTasks()
    val canDoAnything  = completedTasks.nonEmpty || taskExecutionRunner.runningTaskIds.isEmpty

    logger.debug(s"stepExecution: canDoAnything=$canDoAnything")

    if (canDoAnything) {
      // check if we now know about the predecessors for orphan tasks.
      updateOrphans()

      // remove predecessors (dependencies) since some tasks may now be completed
      updatePredecessors()

      // expand all tasks that no longer have dependencies
      invokeCallbacksAndGetTasks()
    }

    // get the running tasks to estimate currently used resources
    val runningTasks: Map[UnitTask, ResourceSet] = runningTasksMap

    // get the tasks that are eligible for execution (tasks with no dependents).  UnitTasks should be scheduled for
    // execution, while other tasks (such as EmptyTask) should be treated as though they are "RUNNING".
    val (toScheduleTasks: Seq[UnitTask], readyToComplete: Seq[UnitTask]) = graphNodesInStateFor(NO_PREDECESSORS)
      .map(_.task).toSeq.partition(_.isInstanceOf[UnitTask])
    logger.debug(s"stepExecution: found ${toScheduleTasks.size} toScheduleTasks tasks")
    logger.debug(s"stepExecution: found ${readyToComplete.size} readyToComplete tasks")

    // get the list of tasks to schedule
    val tasksToSchedule: Map[UnitTask, ResourceSet] = if (!canDoAnything) Map.empty else {
      val tasks = scheduler.schedule(
        runningTasks = runningTasks,
        readyTasks   = toScheduleTasks,
        systemCores  = taskManagerResources.cores,
        systemMemory = taskManagerResources.systemMemory,
        jvmMemory    = taskManagerResources.jvmMemory
      )

      logger.debug("stepExecution: scheduling " + tasks.size + " tasks")

      // add the given tasks to the task runner with the appropriate (as determined by the scheduler) resources.
      scheduleAndRunTasks(tasksToSchedule = tasks)

      tasks
    }

    // for debugging purposes
    logger.debug("stepExecution: finishing one round of execution")
    logger.debug("stepExecution: found " + runningTasks.size + " running tasks and " + tasksToSchedule.size + " tasks to schedule")

    // Update the current sleep time: exponential reduction if we could do **anything**, otherwise linear increase.
    if (canDoAnything) {
      curSleepMilliseconds = Math.max(TaskManager.MinSleepMillis, (curSleepMilliseconds / TaskManager.BackoffSleepFactor).toInt)
    }
    else {
      curSleepMilliseconds = Math.min(TaskManager.MaxSleepMillis, curSleepMilliseconds + TaskManager.StepSleepMillis)
    }

    (
      toScheduleTasks,
      tasksToSchedule.keys,
      runningTasks.keys ++ readyToComplete,
      completedTasks
    )
  }

  override def runToCompletion(failFast: Boolean): BiMap[Task, TaskExecutionInfo] = {
    var allDone = false
    while (!allDone) {

      // Step the execution once
      val startTime = Instant.now()
      val (readyTasks, tasksToSchedule, runningTasks, _) = stepExecution()

      // Warn if the single step in execution "took a long time"
      val stepExecutionDuration = Duration.between(startTime, Instant.now()).getSeconds
      if (stepExecutionDuration > TaskManager.SlowStepTimeSeconds) {
        logger.warning("*" * 80)
        logger.warning(s"A single step in execution was > ${TaskManager.SlowStepTimeSeconds}s (${stepExecutionDuration}s).")
        val infosByStatus: Map[execsystem.TaskStatus.Value, Iterable[TaskExecutionInfo]] = this.taskToInfoBiMapFor.values.groupBy(_.status)
        TaskStatus.values.filter(infosByStatus.contains).foreach { status =>
          logger.warning(s"Found ${infosByStatus(status).size} tasks with status: $status")
        }
        logger.warning("*" * 80)
      }

      logger.debug(s"Sleeping ${curSleepMilliseconds}ms")
      if (curSleepMilliseconds > 0) Thread.sleep(curSleepMilliseconds)

      // check if we have only completed or orphan tasks
      allDone = allGraphNodesInStates(Set(ORPHAN, COMPLETED))

      if (!allDone && runningTasks.isEmpty && tasksToSchedule.isEmpty) {
        if (readyTasks.nonEmpty) {
          logger.error(s"There are ${readyTasks.size} tasks ready to be scheduled but not enough system resources available.")
          readyTasks.foreach { readyTask =>
            val resourcesType: String = readyTask match {
              case _: FixedResources => "FixedResources"
              case _: VariableResources => "VariableResources"
              case _: Schedulable => "Schedulable"
              case _ => "Unknown Type"
            }
            val resources: Option[ResourceSet] = readyTask match {
              case t: Schedulable => t.minResources(new ResourceSet(taskManagerResources.cores, taskManagerResources.systemMemory))
              case _ => None
            }
            val cores  = resources.map(_resources => f"${_resources.cores.value}%.2f").getOrElse("?")
            val memory = resources.map(_.memory.prettyString).getOrElse("?")
            logger.error(s"Task with name '${readyTask.name}' requires $cores cores and $memory memory (task schedulable type: $resourcesType)")
          }
          logger.error(s"There are ${taskManagerResources.cores} core(s) and ${taskManagerResources.systemMemory.prettyString} system memory available.")
        }

        allDone = true
      }
      else if (failFast && hasFailedTasks) {
        allDone = true
      }
    }

    // Output a brief statement about the end state of execution
    logger.info("#" * 80)
    if (graphNodesInStatesFor(List(ORPHAN)).nonEmpty) { // orphaned tasks
      logger.info("Completed execution with orphaned tasks.")
    }
    else if (hasFailedTasks) { // failed tasks
      logger.info("Completed execution with failed tasks.")
    }
    else if (graphNodesInStatesFor(List(COMPLETED)).size != graphNodes.size) { // not all tasks completed
      logger.info("Completed execution with tasks that did not complete.")
    }
    else {
      logger.info("Completed execution successfully.")
    }

    // terminate all running tasks
    for (node <- graphNodes.filter(node => node.state == RUNNING)) {
      terminateTask(node.taskId)
    }

    taskToInfoBiMapFor
  }
}
