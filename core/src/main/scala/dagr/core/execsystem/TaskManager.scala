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
import java.time.Instant

import dagr.core.DagrDef._
import dagr.commons.util.LazyLogging
import dagr.core.tasksystem._
import dagr.commons.util.BiMap
import dagr.commons.io.{Io, PathUtil}

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
    val system   = totalMemory match {
      case Some(memory) if heapSize >= memory => throw new IllegalArgumentException(s"Heap size ${heapSize.prettyString} (-Xmx) must less than the total memory of ${memory.prettyString}.")
      case Some(memory) => memory - heapSize
      case None         => Memory((Resource.systemMemory.bytes * TaskManagerDefaults.defaultSystemMemoryScalingFactor - heapSize.bytes).toLong)
    }
    val jvm      = Memory((heapSize.bytes * TaskManagerDefaults.defaultJvmMemoryScalingFactor).toLong)
    new SystemResources(cores.getOrElse(Resource.systemCores), system, jvm)
  }

  val infinite: SystemResources = SystemResources(Double.MaxValue, Long.MaxValue, Long.MaxValue)
}

case class SystemResources(cores: Cores, systemMemory: Memory, jvmMemory: Memory)

/** Various defaults for task manager */
object TaskManagerDefaults extends LazyLogging {
  /** The default scaling factor for system memory */
  def defaultSystemMemoryScalingFactor: Double = 0.95

  /** The default scaling factor for the memory for the JVM */
  def defaultJvmMemoryScalingFactor: Double = 0.9

  def defaultTaskManagerResources: SystemResources = {
    val resources = SystemResources(cores=None, totalMemory=None) // Let the apply method figure it all out
    logger.info("Defaulting System Resources to " + resources.cores.value + " cores and " + Resource.parseBytesToSize(resources.systemMemory.value) + " memory")
    logger.info("Defaulting JVM Resources to " + Resource.parseBytesToSize(resources.jvmMemory.value) + " memory")
    resources
  }

  /** @return the default scheduler */
  def defaultScheduler: Scheduler = new NaiveScheduler
}

/** Defaults and utility methods for a TaskManager. */
object TaskManager extends LazyLogging {
  import dagr.core.execsystem.TaskManagerDefaults._

  /** Runs a given task to either completion, failure, or inability to schedule.  This will terminate tasks that were still running before returning.
   *
   * @param task the task to run
   * @param sleepMilliseconds the time to wait in milliseconds to wait between trying to schedule tasks.
   * @param taskManagerResources the set of task manager resources, otherwise we use the default
   * @param scriptsDirectory the scripts directory, otherwise we use the default
   * @param logDirectory the log directory, otherwise we use the default
   * @param scheduler the scheduler, otherwise we use the default
   * @param simulate true if we are to simulate running tasks, false otherwise
   * @return a bi-directional map from the set of tasks to their execution information.
   */
  def run(task: Task,
          sleepMilliseconds: Int = 1000,
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
      simulate             = simulate,
      sleepMilliseconds    = sleepMilliseconds
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
  * @param sleepMilliseconds    the time to wait in milliseconds to wait between trying to schedule tasks.
  */
class TaskManager(taskManagerResources: SystemResources = TaskManagerDefaults.defaultTaskManagerResources,
                  scriptsDirectory: Option[Path]             = None,
                  logDirectory: Option[Path]                 = None,
                  scheduler: Scheduler                       = TaskManagerDefaults.defaultScheduler,
                  simulate: Boolean                          = false,
                  sleepMilliseconds: Int                     = 1000
) extends TaskManagerLike with TaskTracker with FinalStatusReporter with LazyLogging {

  private val actualScriptsDirectory = scriptsDirectory getOrElse Io.makeTempDir("scripts")
  protected val actualLogsDirectory  = logDirectory getOrElse Io.makeTempDir("logs")

  private val taskExecutionRunner: TaskExecutionRunnerApi = new TaskExecutionRunner()

  import GraphNodeState._
  import TaskStatus._

  logger.info(s"Executing with ${taskManagerResources.cores} cores and ${taskManagerResources.systemMemory.prettyString} system memory.")
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
  private def logTaskMessage(intro: String, taskInfo: TaskExecutionInfo): Unit = {
    val cores  = taskInfo.resources.cores.toString
    val memory = taskInfo.resources.memory.prettyString
    logger.info(s"$intro '${taskInfo.task.name}' ${taskInfo.status} on attempt #${taskInfo.attemptIndex} with $cores cores and $memory memory")
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

  /** Updates the start and end date for a parent, if it exists */
  private def updateParentStartAndEndDates(node: GraphNode, info: Option[TaskExecutionInfo] = None): Unit = {
    // Set the start and end dates for the parent, if they exist
    node.enclosingNode.foreach { parent =>
      taskExecutionInfoFor(parent).foreach { parentTaskInfo =>
        val taskInfo: TaskExecutionInfo = info.getOrElse(node.taskInfo)
        if (compareOptionalInstants(parentTaskInfo.startDate, taskInfo.startDate) >= 0) {
          parentTaskInfo.startDate = taskInfo.startDate
        }
        if (compareOptionalInstants(parentTaskInfo.endDate, taskInfo.endDate) <= 0) {
          parentTaskInfo.endDate = taskInfo.endDate
        }
      }
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
    logTaskMessage(intro="Task", taskInfo=taskInfo)
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
        throw new RuntimeException("Processing a completed task but it was not done!")
      }
      completeGraphNode(node, Some(taskInfo))
    }
  }

  /** Gets the set of newly completed tasks and updates their state (to either retry or complete)
    *
    * @return for each completed task, a map from a task identifier to a tuple of the exit code and the status of the `onComplete` method
    */
  private def updateCompletedTasks(): Map[TaskId, (Int, Boolean)] = {
    val completedTasks: Map[TaskId, (Int, Boolean)] = taskExecutionRunner.completedTasks()
    completedTasks.keys.foreach(taskId => processCompletedTask(taskId))
    logger.debug("updateCompletedTasks: found " + completedTasks.size + " completed tasks")
    for (taskId <- completedTasks.keys) {
      val name = this(taskId).task.name
      val status = this(taskId).taskInfo.status
      logger.debug("updateCompletedTasks: task [" + name + "] completed with task status [" + status + "]")
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
      logger.debug("updateOrphans: orphan task [" + node.task.name + "] now has [" + node.predecessors.size + "] predecessors")
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
  private def updatePredecessors(): Unit = {
    var hasMore = false
    for (node <- graphNodesWithPredecessors) {
      node.predecessors.filter(p => p.state == GraphNodeState.COMPLETED && TaskStatus.isTaskDone(p.taskInfo.status, failedIsDone=false)).map(p => node.removePredecessor(p))
      logger.debug("runSchedulerOnce: examining task [" + node.task.name + "] for predecessors: " + node.hasPredecessor)
      // - if this node has already been expanded and now has no predecessors, then move it to the next state.
      // - if it hasn't been expanded and now has no predecessors, it should get expanded later
      if (!node.hasPredecessor && node.state == ONLY_PREDECESSORS) {
        val taskInfo = node.taskInfo
        node.task match {
          case task: UnitTask => node.state = NO_PREDECESSORS
          case _ =>
            completeGraphNode(node)
            taskInfo.status = TaskStatus.SUCCEEDED
            hasMore = true // try again for all successors, since we have more nodes that have completed
        }
        logger.debug(s"updatePredecessors: task [${node.task.name}] now has node state [${node.state}] and status [${taskInfo.status}]")
      }
    }

    // If we moved a task straight to the completed state, do this procedure again, as we may have tasks that depend
    // on the former task that can execute because the former task is their last dependency.
    if (hasMore) updatePredecessors()
  }

  /** Invokes `getTasks` on the task associated with the graph node.
    *
    * (1) In the case that `getTasks` returns the same exact task, check for cycles and verify it is a [[UnitTask]].  Since
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
    val tasks = try { node.task.getTasks.toList } catch { case e: Exception => throw new TaskException(e, TaskStatus.FAILED_GET_TASKS) }

    // NB: we don't create a new node for this task if it just returns itself
    // NB: always check for cycles, since we don't know when they could be introduced.  We will check
    //     for cycles in [[addTask]] so only check here if [[getTasks]] returns itself.
    tasks match {
      case Nil => // no tasks returned
        throw new IllegalStateException(s"No tasks to schedule for task: [${node.task.name}]")
      case x :: Nil if x == node.task => // one task and it returned itself
        // check for cycles only when we have a unit task for which calling [[getTasks] returns itself.
        checkForCycles(task = node.task)
        // verify we have a UnitTask
        node.task match {
          case task: UnitTask =>
          case _ => throw new RuntimeException(s"Task was not a UnitTask!")
        }
        false
      case _ =>
        // set the submission time stamp
        taskExecutionInfoFor(node).foreach(info => info.submissionDate = Some(Instant.now()))
        // we will make this task dependent on the tasks it creates...
        if (tasks.contains(node.task)) throw new IllegalStateException(s"Task [${node.task.name}] contained itself in the list returned by getTasks")
        // track the new tasks. If they are already added, that's fine too.
        val taskIds: List[TaskId] = for (task <- tasks) yield addTask(task = task, enclosingNode = Some(node), ignoreExists = true)
        // make this node dependent on those tasks
        taskIds.map(taskId => node.addPredecessors(this(taskId)))
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
      case task: UnitTask => node.state = NO_PREDECESSORS
      case _ =>
        val taskInfo = node.taskInfo
        // In the case that this node is not a [[UnitTask]] we should wait until all of the tasks returned by
        // its [[getTasks]] method have completed, so set it to have only predecessors in that case.
        if (node.hasPredecessor) {
          node.state = GraphNodeState.ONLY_PREDECESSORS
          taskInfo.status = TaskStatus.STARTED
        }
        else { // must have predecessors, since `getTasks` did not return itself, and therefore we made this task dependent on others.
          throw new IllegalStateException(
            "Updating a non-UnitTask's state and status, but could not find any predecessors.  " +
              "Were tasks returned by its get tasks?  " +
              s"Task: [${node.task.name}]")
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
        logTaskMessage(intro="Starting task", taskInfo=taskInfo)
      }
      else {
        completeGraphNode(node, Some(taskInfo))
        logTaskMessage(intro="Could not start task", taskInfo=taskInfo)
      }
    }
  }

  /** Returns a map of running tasks and the resource set they were scheduled with. */
  def runningTasksMap: Map[UnitTask, ResourceSet] = Map(taskExecutionRunner.runningTaskIds.toList.map(id => this(id).task).map(task => (task.asInstanceOf[UnitTask], task.taskInfo.resources)) : _*)

  protected[core] def readyTasksList: List[UnitTask] = graphNodesInStateFor(NO_PREDECESSORS).toList.map(node => node.task.asInstanceOf[UnitTask])

  override def stepExecution(): (Traversable[Task], Traversable[Task], Traversable[Task], Traversable[Task]) = {
    logger.debug("runSchedulerOnce: starting one round of execution")

    // get newly completed tasks
    val completedTasks = updateCompletedTasks()

    // check if we now know about the predecessors for orphan tasks.
    updateOrphans()

    // remove predecessors (dependencies) since some tasks may now be completed
    updatePredecessors()

    // expand all tasks that no longer have dependencies
    invokeCallbacksAndGetTasks()

    // get the running tasks to estimate currently used resources
    val runningTasks: Map[UnitTask, ResourceSet] = runningTasksMap

    // get the tasks that are eligible for execution (tasks with no dependents)
    val readyTasks: List[UnitTask] = graphNodesInStateFor(NO_PREDECESSORS).toList.map(node => node.task.asInstanceOf[UnitTask])
    logger.debug("runSchedulerOnce: found " + readyTasks.size + " readyTasks tasks")

    // get the list of tasks to schedule
    val tasksToSchedule: Map[UnitTask, ResourceSet] = scheduler.schedule(
      runningTasks = runningTasks,
      readyTasks   = readyTasks,
      systemCores  = taskManagerResources.cores,
      systemMemory = taskManagerResources.systemMemory,
      jvmMemory    = taskManagerResources.jvmMemory
    )
    logger.debug("runSchedulerOnce: scheduling " + tasksToSchedule.size + " tasks")

    // add the given tasks to the task runner with the appropriate (as determined by the scheduler) resources.
    scheduleAndRunTasks(tasksToSchedule = tasksToSchedule)

    // for debugging purposes
    logger.debug("runSchedulerOnce: finishing one round of execution")
    logger.debug("runSchedulerOnce: found " + runningTasks.size + " running tasks and " + tasksToSchedule.size + " tasks to schedule")

    (
      readyTasks,
      tasksToSchedule.keys,
      runningTasks.keys,
      completedTasks.keys.map(taskId => this(taskId).task)
    )
  }

  override def runToCompletion(failFast: Boolean): BiMap[Task, TaskExecutionInfo] = {
    var allDone = false
    while (!allDone) {
      val (readyTasks, tasksToSchedule, runningTasks, _) = stepExecution()

      Thread.sleep(sleepMilliseconds)

      // check if we have only completed or orphan all tasks
      allDone = graphNodesInStatesFor(List(ORPHAN, GraphNodeState.COMPLETED)).size == graphNodes.size

      if (!allDone && runningTasks.isEmpty && tasksToSchedule.isEmpty) {
        logger.error(s"There are ${readyTasks.size} tasks ready to be scheduled but not enough system resources available.")
        readyTasks.foreach { readyTask =>
          val (resourcesType: String, resources: Option[ResourceSet]) = readyTask match {
            case t: FixedResources    => ("FixedResources", Some(t.resources))
            case t: VariableResources => ("VariableResources", Some(t.resources))
            case t: Schedulable       => ("Schedulable", t.pickResources(new ResourceSet(Cores(Integer.MAX_VALUE), Memory.infinite)))
            case t                    => ("Unknown Type", None)
          }
          val cores  = resources.map(_.cores.toString).getOrElse("?")
          val memory = resources.map(_.memory.prettyString).getOrElse("?")
          logger.error(s"Task with name '${readyTask.name}' requires $cores cores and $memory memory (task schedulable type: $resourcesType)")
        }
        logger.error(s"There are ${taskManagerResources.cores} core(s) and ${taskManagerResources.systemMemory.prettyString} system memory available.")
        allDone = true
      }
      else if (failFast && hasFailedTasks) {
        allDone = true
      }
    }

    // terminate all running tasks
    for (node <- graphNodes.filter(node => node.state == RUNNING)) {
      terminateTask(node.taskId)
    }

    taskToInfoBiMapFor
  }
}
