/*
 * The MIT License
 *
 * Copyright (c) 2015 Fulcrum Genomics LLC
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

import java.nio.file.{Path, Paths}
import java.sql.Timestamp

import dagr.core.tasksystem.{Task, UnitTask}
import dagr.core.util.{Io, BiMap, LazyLogging, PathUtil}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** The resources needed for the task manager */
object TaskManagerResources {
  /* Creates a new TaskManagerResources that is a copy of an existing one. */
  def apply(that: TaskManagerResources): TaskManagerResources = {
    new TaskManagerResources(cores = that.cores, systemMemory = that.systemMemory, jvmMemory = that.jvmMemory)
  }

  /** Creates a new TaskManagerResources with the specified values. */
  def apply(cores: Float, systemMemory: Long, jvmMemory: Long): TaskManagerResources = {
    new TaskManagerResources(cores = Cores(cores), systemMemory = Memory(systemMemory), jvmMemory = Memory(jvmMemory))
  }

  /** Creates a new TaskManagerResources with the specified values. */
  def apply(cores: Cores, systemMemory: Memory, jvmMemory: Memory): TaskManagerResources = {
    new TaskManagerResources(cores, systemMemory, jvmMemory: Memory)
  }

  /** Creates a new TaskManagerResources with the cores provided and partitions the memory bewteen system and JVM. */
  def apply(cores: Option[Cores] = None, totalMemory: Option[Memory] = None) : TaskManagerResources = {
    val heapSize = Resource.heapSize
    val system   = totalMemory match {
      case Some(memory) => memory - heapSize
      case None         => Memory((Resource.systemMemory.bytes * TaskManagerDefaults.defaultSystemMemoryScalingFactor - heapSize.bytes).toLong)
    }
    val jvm      = Memory((heapSize.bytes * TaskManagerDefaults.defaultJvmMemoryScalingFactor).toLong)
    new TaskManagerResources(cores.getOrElse(Resource.systemCores), system, jvm)
  }

  val infinite: TaskManagerResources = TaskManagerResources(Float.MaxValue, Long.MaxValue, Long.MaxValue)
}

class TaskManagerResources(val cores: Cores, val systemMemory: Memory, val jvmMemory: Memory)

/** Various defaults for task manager */
object TaskManagerDefaults extends LazyLogging {
  /** The default scaling factor for system memory */
  var defaultSystemMemoryScalingFactor: Double = 0.95

  /** The default scaling factor for the memory for the JVM */
  var defaultJvmMemoryScalingFactor: Double = 0.9

  def defaultTaskManagerResources: TaskManagerResources = {
    val resources = TaskManagerResources(cores=None, totalMemory=None) // Let the apply method figure it all out
    logger.info("Defaulting System Resources to " + resources.cores.value + " cores and " + Resource.parseBytesToSize(resources.systemMemory.value) + " memory")
    logger.info("Defaulting JVM Resources to " + Resource.parseBytesToSize(resources.jvmMemory.value) + " memory")
    resources
  }

  /** @return the default scheduler */
  def defaultScheduler: Scheduler = new NaiveScheduler
}

/** Defaults and utility methods for a TaskManager. */
object TaskManager extends LazyLogging {

  import TaskManagerDefaults._
  import dagr.core.util.StringUtil._

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
          taskManagerResources: Option[TaskManagerResources] = Some(defaultTaskManagerResources),
          scriptsDirectory: Option[Path] = None,
          logDirectory: Option[Path] = None,
          scheduler: Option[Scheduler] = Some(defaultScheduler),
          simulate: Boolean = false): BiMap[Task, TaskExecutionInfo] = {

    val taskManager: TaskManager = new TaskManager(
      taskManagerResources = taskManagerResources.getOrElse(defaultTaskManagerResources),
      scriptsDirectory = scriptsDirectory,
      logDirectory = logDirectory,
      scheduler = scheduler.getOrElse(defaultScheduler),
      simulate = simulate
    )

    taskManager.addTask(ignoreExists = false, task = task)
    taskManager.runAllTasks(sleepMilliseconds = sleepMilliseconds)

    taskManager.getTaskToInfoBiMap
  }

  /** Gets the time stamp as a string (without Nanoseconds), or NA if it is None */
  private def timestampStringOrNA(timestamp: Option[Timestamp]): String = {
    timestamp match {
      case Some(ts) =>
        val str = ts.toString
        str.substring(0, str.lastIndexOf('.')) // remove nanos
      case None => "NA"
    }
  }


  /** Writes a delimited string of the status of all tasks managed
    *
    * @param taskManagerState the task manager
    * @param loggerMethod the method to use to write task status information, one line at a time
    * @param delimiter the delimiter between entries in a row
    */
  def logTaskStatusReport(taskManagerState: TaskManagerState,
                          loggerMethod: String => Unit,
                          delimiter: String = "  "): Unit = {
    val taskInfoMap: BiMap[Task, TaskExecutionInfo] = taskManagerState.getTaskToInfoBiMap


    // Create the task status table
    val taskStatusTable: ListBuffer[List[String]] = new ListBuffer[List[String]]()
    taskStatusTable.append(List(
      "ID", "NAME", "STATUS", "CORES", "MEMORY",
      "SUBMISSION_DATE", "START_DATE", "END_DATE",
      "EXECUTION_TIME", "TOTAL_TIME",
      "SCRIPT", "LOG", "ATTEMPT_INDEX", "DEBUG_STATE"
    ))
    val taskStatusMap: scala.collection.mutable.Map[TaskStatus.Value, Int] = mutable.HashMap[TaskStatus.Value, Int]()
    TaskStatus.values.foreach(status => taskStatusMap.put(status, 0))
    for (taskInfo <- taskInfoMap.values.toList.sortBy(taskInfo => taskInfo.id)) {
      // get the total execution time, and total time since submission
      val (executionTime: String, totalTime: String) = taskInfo.endDate match {
        case Some(end) => (
          formatElapsedTime((end.getTime - taskInfo.startDate.get.getTime) / 1000),
          formatElapsedTime((end.getTime - taskInfo.submissionDate.get.getTime) / 1000)
          )
        case None => ("NA", "NA")
      }
      val graphNodeState = taskManagerState.getGraphNodeState(taskInfo.id) match {
        case Some(state) => state.toString
        case None => "NA"
      }

      val row: List[String] = List(
        taskInfo.id.toString(),
        taskInfo.task.name,
        taskInfo.status.toString,
        f"${taskInfo.resources.cores.cores}%.2f",
        taskInfo.resources.memory.prettyString,
        timestampStringOrNA(taskInfo.submissionDate),
        timestampStringOrNA(taskInfo.startDate),
        timestampStringOrNA(taskInfo.endDate),
        executionTime,
        totalTime,
        taskInfo.script.toFile.getAbsolutePath,
        taskInfo.logFile.toFile.getAbsolutePath,
        taskInfo.attemptIndex,
        graphNodeState).map(_.toString)
      taskStatusTable.append(row)

      taskStatusMap.put(taskInfo.status, taskStatusMap.get(taskInfo.status).get + 1)
    }

    // Write the task status table
    loggerMethod(columnIt(taskStatusTable.toList, delimiter))

    // Create and write the task status counts
    val taskStatusCountTable = new ListBuffer[List[String]]()
    val keys = taskStatusMap.keys.toList.filter(status => taskStatusMap.getOrElse(status, 0) > 0)
    taskStatusCountTable.append(keys.map(_.toString))
    taskStatusCountTable.append(keys.map(status => taskStatusMap.getOrElse(status, 0).toString))
    loggerMethod("\n" + columnIt(taskStatusCountTable.toList, delimiter))
  }
}

/** A manager of tasks.
  *
  * No validation of whether or not we have the provided system or in-Jvm resources will occur.
  *
  * @param taskManagerResources the set of task manager resources, otherwise we use the default
  * @param scriptsDirectory     the scripts directory, otherwise a temporary directory will be used
  * @param logDirectory         the log directory, otherwise a temporary directory will be used
  * @param scheduler            the scheduler, otherwise we use the default
  * @param simulate             true if we are to simulate running tasks, false otherwise
  */
class TaskManager(taskManagerResources: TaskManagerResources = TaskManagerDefaults.defaultTaskManagerResources,
                  scriptsDirectory: Option[Path]             = None,
                  logDirectory: Option[Path]                 = None,
                  scheduler: Scheduler                       = TaskManagerDefaults.defaultScheduler,
                  simulate: Boolean                          = false
) extends TaskManagerState with LazyLogging {

  val actualScriptsDirectory = scriptsDirectory getOrElse Io.makeTempDir("scripts")
  val actualLogsDirectory    = logDirectory getOrElse Io.makeTempDir("logs")

  private val taskRunner: TaskRunner = new TaskRunner()

  import GraphNodeState._
  import TaskStatus._

  logger.info("Script files will be written to: " + actualScriptsDirectory)
  logger.info("Logs will be written to: " + actualLogsDirectory)

  def getTaskManagerResources: TaskManagerResources = TaskManagerResources(taskManagerResources)

  private def getFile(task: Task, taskId: BigInt, directory: Path, ext: String): Path = {
    val sanitizedName: String = PathUtil.sanitizeFileName(task.name)
    Paths.get(directory.toString, s"$sanitizedName.$taskId.$ext")
  }

  override protected def getTaskScript(task: Task, taskId: BigInt): Path = getFile(task, taskId, actualScriptsDirectory, "sh")
  override protected def getTaskLogFile(task: Task, taskId: BigInt): Path = getFile(task, taskId, actualLogsDirectory, "log")

  /** Replace the original task with the replacement task and update
   * any internal references.  This will terminate the task if it is running.
   *
   * @param original the original task to replace.
   * @param replacement the replacement task.
   * @return true if we are successful, false if the original is not being tracked.
   */
  override def replaceTask(original: Task, replacement: Task): Boolean = {
    if (!hasTask(original)) return false

    val taskId: BigInt = getTaskId(original).get
    val taskInfo: TaskExecutionInfo = getTaskExecutionInfo(taskId).get

    if (taskInfo.status == STARTED) { // the task has been started, kill it
      taskRunner.terminateTask(taskId)
      processCompletedTask(taskId = taskId, doRetry = false)
    }

    super.replaceTask(original = original, replacement = replacement)
    true
  }

  /** Resubmit a task for execution.  This will stop the task if it is currently running, and queue
   * it up for execution.  The number of attempts will be reset to zero.
   *
   * @param task the task to resubmit.
   * @return true if the task was successfully resubmitted, false otherwise.
   */
  def resubmitTask(task: Task): Boolean = {
    val taskId: BigInt = getTaskId(task).getOrElse(-1)
    if (taskId < 0) return false
    val taskInfo: TaskExecutionInfo = getTaskExecutionInfo(taskId).get
    val node: GraphNode = getGraphNode(taskId).get

    // check if the task is running and if so, kill it
    if (taskInfo.status == STARTED) {
      taskRunner.terminateTask(taskId)
      processCompletedTask(taskId = taskId, doRetry = false)
    }

    // reset the internal data structures for this task
    taskInfo.status = TaskStatus.UNKNOWN
    taskInfo.attemptIndex = 1
    node.state = NO_PREDECESSORS

    true
  }

  /** Compares two timestamp options.  If either option is empty, zero is returned. */
  private def compareOptionalTimestamps(left: Option[Timestamp], right: Option[Timestamp]): Int =  {
    left.foreach { l =>
      right.foreach { r =>
        return l.compareTo(r)
      }
    }
    0
  }

  /** Updates the start and end date for a parent, if it exists */
  private def updateParentStartAndEndDates(node: GraphNode, info: Option[TaskExecutionInfo] = None): Unit = {
    // Set the start and end dates for the parent, if they exist
    node.parent.foreach { parent =>
      getTaskExecutionInfo(parent).foreach { parentTaskInfo =>
        val taskInfo: TaskExecutionInfo = info.getOrElse(getTaskExecutionInfo(node).get)
        if (compareOptionalTimestamps(parentTaskInfo.startDate, taskInfo.startDate) >= 0) {
          parentTaskInfo.startDate = taskInfo.startDate
        }
        if (compareOptionalTimestamps(parentTaskInfo.endDate, taskInfo.endDate) <= 0) {
          parentTaskInfo.endDate = taskInfo.endDate
        }
      }
    }
  }

  /** Sets the state of the node to completed, and updates the start and end date of any parent */
  private def setGraphNodeToCompleted(node: GraphNode,  info: Option[TaskExecutionInfo] = None): Unit = {
    updateParentStartAndEndDates(node, info)
    node.state = GraphNodeState.COMPLETED
  }

  /** Update internal data structures for the completed task.  If a task has failed,
   * retry it.
   *
   * @param taskId the task identifier for the completed task.
   */
  private def processCompletedTask(taskId: BigInt, doRetry: Boolean = true): Unit = {
    val node: GraphNode = getGraphNode(taskId = taskId).get
    val taskInfo: TaskExecutionInfo = getTaskExecutionInfo(taskId).get
    logger.info("processCompletedTask: Task [" + taskInfo.task.name + "] had TaskStatus=" + taskInfo.status.toString)
    if (TaskStatus.isTaskFailed(taskStatus = taskInfo.status) && doRetry) {
      // Only if it has failed
      val task: Option[Task] = taskInfo.task.retry(taskInfo, taskInfo.status == TaskStatus.FAILED_ON_COMPLETE)
      if (task.isDefined) {
        // retry it
        logger.debug("task [" + task.get.name + "] is being retried")
        node.state = NO_PREDECESSORS
        taskInfo.attemptIndex += 1
        return
      }
      else {
        logger.debug("task [" + taskInfo.task.name + "] is *not* being retried")
      }
    }
    logger.debug("processCompletedTask: Task [" + taskInfo.task.name + "] updating node to completed")
    if (TaskStatus.isTaskNotDone(taskInfo.status, failedIsDone = true)) {
      throw new RuntimeException("Processing a completed task but it was not done!")
    }
    setGraphNodeToCompleted(node, Some(taskInfo))
  }

  /** Gets the set of newly completed tasks and updates their state (to either retry or complete)
    *
    * @param timeout the length of time in milliseconds to wait for running tasks to complete
    * @return for each completed task, a map from a task identifier to a tuple of the exit code and the status of the `onComplete` method
    */
  private def updateCompletedTasks(timeout: Int = 1000): Map[BigInt, (Int, Boolean)] = {
    val completedTasks: Map[BigInt, (Int, Boolean)] = taskRunner.getCompletedTasks(timeout = timeout)
    completedTasks.keys.foreach(taskId => processCompletedTask(taskId))
    logger.debug("updateCompletedTasks: found " + completedTasks.size + " completed tasks")
    for (taskId <- completedTasks.keys) {
      logger.debug("updateCompletedTasks: task [" + getTask(taskId).get.name + "] completed with task status [" + getTaskStatus(taskId).get + "]")
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
    getGraphNodesInState(ORPHAN).filter(node => allPredecessorsAdded(task = node.task)).foreach(node => {
      // add the predecessors
      logger.debug("updateOrphans: found an orphan task [" + node.task.name + "] that has [" + getPredecessors(task=node.task).get.size + "] predecessors")
      node.addPredecessors(getPredecessors(task=node.task).get)
      logger.debug("updateOrphans: orphan task [" + node.task.name + "] now has [" + node.getPredecessors.size + "] predecessors")
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
    for (node <- getGraphNodesWithPredecessors) {
      node.getPredecessors.filter(p => p.state == GraphNodeState.COMPLETED).map(p => node.removePredecessor(p))
      logger.debug("runSchedulerOnce: examining task [" + node.task.name + "] for predecessors: " + node.hasPredecessor)
      // - if this node has already been expanded and now has no predecessors, then move it to the next state.
      // - if it hasn't been expanded and now has no predecessors, it should get expanded later
      if (!node.hasPredecessor && node.state == ONLY_PREDECESSORS) {
        node.task match {
          case task: UnitTask => node.state = NO_PREDECESSORS
          case _ =>
            setGraphNodeToCompleted(node)
            getTaskExecutionInfo(node).get.status = TaskStatus.SUCCEEDED
            hasMore = true // try again for all successors, since we have more nodes that have completed
        }
        logger.debug(s"updatePredecessors: task [${node.task.name}] now has node state [${node.state}] and status [" + getTaskExecutionInfo(node).get.status + "]")
      }
    }

    // If we moved a task straight to the completed state, do this procedure again, as we may have tasks that depend
    // on the former task that can execute because the former task is their last dependency.
    if (hasMore) updatePredecessors()
  }

  /** For all unexpanded tasks with no predecessors, invoke any callbacks (`invokeCallbacks`) and get any tasks
    * they generate (`getTasks`).  This will add those tasks to the task manager.
    *
    */
  private def invokeCallbacksAndGetTasks(): Unit = {
    var hasMore = false
    // find all tasks that are unexpanded and previously have dependencies (predecessors) but no longer do.
    for (node <- getGraphNodesInState(PREDECESSORS_AND_UNEXPANDED).filterNot(_.hasPredecessor)) {
      logger.debug("invokeCallbacksAndGetTasks: found node in state PREDECESSORS_AND_UNEXPANDED with no predecessors: task [" + node.task.name + "]")
      try {
        // invoke any callbacks needed
        try { node.task.invokeCallbacks() }
        catch { case e: Exception => throw new TaskException(e, TaskStatus.FAILED_CALLBACKS) }

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
          case _ =>
            // set the submission time stamp
            getTaskExecutionInfo(node).foreach(info => info.submissionDate = Some(new Timestamp(System.currentTimeMillis)))
            // we will make this task dependent on the tasks it creates...
            if (tasks.contains(node.task)) throw new IllegalStateException(s"Task [${node.task.name}] contained itself in the list returned by getTasks")
            // track the new tasks. If they are already added, that's fine too.
            val taskIds: List[BigInt] = for (task <- tasks) yield addTask(task = task, parent = Some(node), ignoreExists = true)
            // make this node dependent on those tasks
            taskIds.map(taskId => node.addPredecessors(getGraphNode(taskId).get))
            // TODO: we could check each new task to see if they are in the PREDECESSORS_AND_UNEXPANDED state
            hasMore = true
        }

        // update this node's state
        node.task match {
          case task: UnitTask => node.state = NO_PREDECESSORS
          case _ =>
            val taskInfo: TaskExecutionInfo = getTaskExecutionInfo(node).get
            // In the case that this node is not a [[UnitTask]] we should wait until all of the tasks returned by
            // its [[getTasks]] method have completed, so set it to have only dependents in that case.  If the
            if (node.hasPredecessor) {
              node.state = GraphNodeState.ONLY_PREDECESSORS
              taskInfo.status = TaskStatus.STARTED
            }
            else { // move it to a complete state.  TODO: is this reachable given tasks.isEmpty is checked above?
              setGraphNodeToCompleted(node, Some(taskInfo))
              taskInfo.status = TaskStatus.SUCCEEDED
            }
        }
      }
      catch {
        // Catch any exception so we can just fail this task.
        // TODO: we could fail the callbacks too, so perhaps we want to separate that case from failing get tasks?
        case e: Exception =>
          val taskInfo: TaskExecutionInfo = getTaskExecutionInfo(node).get
          setGraphNodeToCompleted(node, Some(taskInfo))
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
      val taskInfo = getTaskExecutionInfo(task).get
      taskInfo.resources = taskResourceSet // update the resource set that was used when scheduling the task
      val node = getGraphNode(task).get
      if (taskRunner.runTask(taskInfo, simulate)) {
        node.state = RUNNING
      }
      else {
        setGraphNodeToCompleted(node, Some(taskInfo))
      }
      logger.info("scheduleAndRunTasks: running task [" + task.name + "] state [" + node.state + "]")
    }
  }

  /** Run an iteration of managing tasks.
   *
   * 1. Get tasks that have completed and update their state.
   * 2. Update any resolved dependencies in the execution graph.
   * 3. Get tasks for any task that has no predecessors until no more can be found.
   * 4. Schedule tasks and run them.
   *
   * @param timeout the length of time in milliseconds to wait for running tasks to complete
   * @return a tuple of (1) tasks that can be scheduled, (2) tasks that were scheduled, (3) tasks that are running prior to scheduling, and (4) the tasks that have completed prior to scheduling.
   */
  def runSchedulerOnce(timeout: Int = 1000): (List[Task], List[Task], List[Task], List[Task]) = {
    logger.debug("runSchedulerOnce: starting one round of execution")

    // get newly completed tasks
    val completedTasks = updateCompletedTasks(timeout = timeout)

    // check if we now know about the predecessors for orphan tasks.
    updateOrphans()

    // remove predecessors (dependencies) since some tasks may now be completed
    updatePredecessors()

    // expand all tasks that no longer have dependencies
    invokeCallbacksAndGetTasks()

    // get the running tasks to estimate currently used resources
    val runningTasks: Map[UnitTask, ResourceSet] = Map(
      taskRunner.getRunningTasks.toList
        .map(taskId => getTaskExecutionInfo(taskId).get)
        .map(info => (info.task.asInstanceOf[UnitTask], info.resources)) : _*)

    // get the tasks that are eligible for execution (tasks with no dependents)
    val readyTasks: List[UnitTask] = getGraphNodesInState(NO_PREDECESSORS).toList.map(node => node.task.asInstanceOf[UnitTask])
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

    (readyTasks, tasksToSchedule.keys.toList, runningTasks.keys.toList, completedTasks.keys.map(taskId => getTask(taskId).get).toList)
  }

  /** Run all tasks managed to either completion, failure, or inability to schedule.  This will terminate tasks that were still running before returning.
    *
    * @param sleepMilliseconds the time to wait in milliseconds to wait between trying to schedule tasks.
    * @param timeout           the length of time in milliseconds to wait for running tasks to complete
    * @return a bi-directional map from the set of tasks to their execution information.
    */
  def runAllTasks(sleepMilliseconds: Int = 1000, timeout: Int = 1000): BiMap[Task, TaskExecutionInfo] = {
    var allDone = false
    while (!allDone) {
      val (readyTasks, tasksToSchedule, runningTasks, _) = runSchedulerOnce(timeout = timeout)

      Thread.sleep(sleepMilliseconds)

      // check if we have only completed or orphan all tasks
      allDone = getGraphNodesInStates(List(ORPHAN, GraphNodeState.COMPLETED)).size == getGraphNodes.size

      if (!allDone && runningTasks.isEmpty && tasksToSchedule.isEmpty) {
        logger.error("No running tasks and no tasks scheduled but had " + readyTasks.size + " tasks ready to be scheduled")
        readyTasks.foreach(readyTask => {
          logger.error(readyTask.toString + " tasks status: " + getTaskStatus(readyTask) + " graph state: " + getGraphNodeState(readyTask))
        })
        allDone = true
      }
      else if (hasFailedTasks) {
        allDone = true
      }
    }

    // terminate all running tasks
    for (node <- getGraphNodes.filter(node => node.state == RUNNING)) {
      taskRunner.terminateTask(node.taskId)
      processCompletedTask(taskId = node.taskId, doRetry = false)
    }

    getTaskToInfoBiMap
  }
}
