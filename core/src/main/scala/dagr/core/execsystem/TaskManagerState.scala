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

import java.nio.file.Path
import java.sql.Timestamp

import dagr.core.execsystem.TaskStatus._
import dagr.core.tasksystem.Task
import dagr.core.util.{BiMap, LazyLogging}

/** Stores the state of the execution graph. */
trait TaskManagerState extends LazyLogging {

  private var nextTaskId: BigInt = 0

  /**
    * We have 1:1 mappings for the following:
    *
    * Task Id <-> Task
    * Task Id <-> TaskExecutionInfo
    * Task Id <-> GraphNode
    * Task <-> GraphNode
    *
    */
  private val tasksToIds     = new BiMap[Task, BigInt]()
  private val taskInfosToIds = new BiMap[TaskExecutionInfo, BigInt]()
  private val nodesToIds     = new BiMap[GraphNode, BigInt]()
  private val tasksToNodes   = new BiMap[Task, GraphNode]  // Could be replaced with use of tasksToIds and nodesToIds

  /** Generates a path to a file to hold the task command */
  protected def scriptPathFor(task: Task, taskId: BigInt): Path

  /** Generates a path to file to store the log output of the task */
  protected def logPathFor(task: Task, taskId: BigInt): Path

  /** Checks for cycles in the graph to which the task belongs.
    *
    * Throws an [[IllegalArgumentException]] if a cycle was found after logging each strongly connected component with
    * a cycle in the graph.
    *
    * @param task a task in the graph to check.
    */
  protected def checkForCycles(task: Task): Unit = {
    // check for cycles
    if (Task.hasCycle(task)) {
      logger.error("Task was part of a graph that had a cycle")
      for (component <- Task.findStronglyConnectedComponents(task = task)) {
        if (Task.isComponentACycle(component = component)) {
          logger.error("Tasks were part of a strongly connected component with a cycle: "
            + component.map(t => s"[${t.name}]").mkString(", "))
        }
      }
      throw new IllegalArgumentException(s"Task was part of a graph that had a cycle [${task.name}]")
    }
  }

  /** Adds a task to be managed
    *
    * Throws an [[IllegalArgumentException]] if a cycle was found after logging each strongly connected component with
    * a cycle in the graph.
    *
    * @param ignoreExists true if we just return the task id for already added tasks, false if we are to throw an [[IllegalArgumentException]]
    * @param task the given task.
    * @return the task identifier.
    */
  def addTask(task: Task, parent: Option[GraphNode] = None, ignoreExists: Boolean = false): BigInt = {
    val nowTimestamp = new Timestamp(System.currentTimeMillis)

    if (hasTaskId(nextTaskId)) throw new IllegalArgumentException(s"Task [${task.name}] with id [$nextTaskId] was already added!")

    taskIdFor(task) match {
      case Some(taskId) if ignoreExists => taskId
      case Some(taskId) => throw new IllegalArgumentException(s"Task [${task.name}] was already added!")
      case None =>
        // check for cycles
        checkForCycles(task = task)

        // get the task id
        val taskId = nextTaskId
        nextTaskId += 1

        // make a new task info and node
        val taskInfo = new TaskExecutionInfo(
          task           = task,
          id             = taskId,
          status         = UNKNOWN,
          script         = scriptPathFor(task = task, taskId = taskId),
          logFile        = logPathFor(task, taskId = taskId),
          submissionDate = Some(nowTimestamp)
        )

        // check for orphans (state == ORPHAN)
        // we could call allPredecessorsAdded, but this saves on some fn calls
        val node: GraphNode = this.predecessorsOf(task = task) match {
          case None => new GraphNode(taskId=taskId, task=task, predecessorNodes=Nil, state=GraphNodeState.ORPHAN, parent=parent)
          case Some(predecessors) => new GraphNode(taskId=taskId, task=task, predecessorNodes=predecessors, parent=parent)
        }

        tasksToIds.add(task, taskId)
        taskInfosToIds.add(taskInfo, taskId)
        nodesToIds.add(node, taskId)
        tasksToNodes.add(task, node)

        taskId
    }
  }

  /** Adds tasks to be managed
    *
    * @param tasks the given tasks.
    * @param ignoreExists true if we just return the task id for already added tasks, false if we are to throw an [[IllegalArgumentException]]
    * @return the task identifiers.
    */
  def addTasks(tasks: Traversable[Task], parent: Option[GraphNode] = None, ignoreExists: Boolean = false): List[BigInt] = {
    tasks.map(task => addTask(task=task, parent=parent, ignoreExists=ignoreExists)).toList
  }

  /** Adds tasks to be managed
    *
    * if the task id for already added tasks this will throw an [[IllegalArgumentException]]
    *
    * @param tasks the given tasks.
    * @return the task identifiers.
    */
  def addTasks(tasks: Task*): List[BigInt] = {
    tasks.map(task => addTask(task, ignoreExists=false)).toList
  }

  /** Checks if a task is being tracked.
    *
    * @param task the task to test
    * @return true if the task is being tracked, false otherwise
    */
  def hasTask(task: Task): Boolean = tasksToIds.containsKey(task)

  /** Checks if a task with the given identifier is being tracked.
    *
    * @param taskId the task identifier to test
    * @return true if the task is being tracked, false otherwise
    */
  def hasTaskId(taskId: BigInt): Boolean = tasksToIds.containsValue(taskId)

  /** Gets the task associated with the identifier, if any
    *
    * @param taskId the task id
    * @return the task associate with the id if found, None otherwise
    */
  def taskFor(taskId: BigInt): Option[Task] = tasksToIds.keyFor(taskId)

  /** Gets the task associated with the graph node
    *
    * @param node the node to lookup
    * @return the task if found, None otherwise
    */
  def taskFor(node: GraphNode): Option[Task] = {
    nodesToIds.valueFor(node) match {
      case Some(taskId: BigInt) => tasksToIds.keyFor(taskId)
      case _ => None
    }
  }

  /** Get the task's execution information if it exists.
    *
    * @param taskId the task identifier.
    * @return the task execution information if the task is managed, None otherwise
    */
  def taskExecutionInfoFor(taskId: BigInt): Option[TaskExecutionInfo] = taskInfosToIds.keyFor(taskId)

  /** Get the task's execution information if it exists.
    *
    * @param task the task.
    * @return the task execution information if the task is managed, None otherwise
    */
  def taskExecutionInfoFor(task: Task): Option[TaskExecutionInfo] = {
    tasksToIds.valueFor(task) match {
      case Some(taskId: BigInt) => taskInfosToIds.keyFor(taskId)
      case _ => None
    }
  }

  /** Get the task's execution information if it exists.
    *
    * @param node the node associated with a task.
    * @return the task execution information if the task is managed, None otherwise
    */
  def taskExecutionInfoFor(node: GraphNode): Option[TaskExecutionInfo] = {
    nodesToIds.valueFor(node) match {
      case Some(taskId: BigInt) => taskInfosToIds.keyFor(taskId)
      case _ => None
    }
  }

  /** Get the task's associated [[TaskStatus]].
    *
    * @param task the task.
    * @return the task status if the task is managed, None otherwise.
    */
  def taskStatusFor(task: Task): Option[TaskStatus.Value] = {
    taskExecutionInfoFor(task) match {
      case Some(taskInfo: TaskExecutionInfo) => Some(taskInfo.status)
      case _ => None
    }
  }

  /** Get the task's associated [[TaskStatus]].
    *
    * @param taskId the task identifier.
    * @return the task status if the task is managed, None otherwise.
    */
  def taskStatusFor(taskId: BigInt): Option[TaskStatus.Value] = {
    taskExecutionInfoFor(taskId) match {
      case Some(taskInfo: TaskExecutionInfo) => Some(taskInfo.status)
      case _ => None
    }
  }

  /** Get the task identifier for the given task.
    *
    * @param task the task.
    * @return the task identifier, None if the task is not being managed.
    */
  def taskIdFor(task: Task): Option[BigInt] = tasksToIds.valueFor(task)

  /** Get the task identifiers for all tracked tasks.
   *
   * @return the task ids
   */
  def taskIdsFor(): Iterable[BigInt] = tasksToIds.values

  /** Get the bi-directional map between managed tasks and their associated task execution information.
    *
    * @return the task and task execution information bi-directional map.
    */
  def taskToInfoBiMapFor: BiMap[Task, TaskExecutionInfo] = {
    val map: BiMap[Task, TaskExecutionInfo] = new BiMap[Task, TaskExecutionInfo]()
    for ((task, taskId) <- tasksToIds) {
      map.add(task, taskInfosToIds.keyFor(taskId).get)
    }
    map
  }

  /** Get the graph node associated with the task identifier
    *
    * @param taskId the task identifier
    * @return the graph node associated with the task identifier if found, None otherwise
    */
  def graphNodeFor(taskId: BigInt): Option[GraphNode] = nodesToIds.keyFor(taskId)

  /** Get the graph node associated with the task
    *
    * @param task a task in the graph
    * @return the graph node associated with the task if found, None otherwise
    */
  def graphNodeFor(task: Task): Option[GraphNode] = tasksToNodes.valueFor(task)

  /** Get the execution state of the task.
    *
    * @param taskId the task identifier.
    * @return the execution state of the task.
    */
  def graphNodeStateFor(taskId: BigInt): Option[GraphNodeState.Value] = {
    nodesToIds.keyFor(taskId) match {
      case Some(node: GraphNode) => Some(node.state)
      case _ => None
    }
  }

  /** Get the execution state of the task.
    *
    * @param task the task.
    * @return the execution state of the task.
    */
  def graphNodeStateFor(task: Task): Option[GraphNodeState.Value] = {
    tasksToIds.valueFor(task) match {
      case Some(taskId: BigInt) => graphNodeStateFor(taskId)
      case _ => None
    }
  }

  /** Get the graph nodes in the execution graph.
    *
    * @return the graph nodes, in no particular order.
    */
  def graphNodes: Iterable[GraphNode] = nodesToIds.keys

  /** Replace the original task with the replacement task and update
    * any internal references.  If the task failed, it will requeue
    * the task for execution.  The new task should have no predecessors,
    * and will inherit the predecessors of the original.  If the original
    * task had child tasks, those will not be changed.
    *
    * @param original the original task to replace.
    * @param replacement the replacement task.
    * @return true if we are successful, false if the original is not being
    *         tracked or the replacement has predecessors.
    */
  def replaceTask(original: Task, replacement: Task): Boolean = {

    // TODO: how to replace UnitTask vs. Workflows

    taskIdFor(original) match {
      case None => false
      case Some(originalTaskId: BigInt) =>
        if (replacement.tasksDependedOn.nonEmpty || replacement.tasksDependingOnThisTask.nonEmpty) false
        else {
          val taskInfo: TaskExecutionInfo = taskExecutionInfoFor(originalTaskId).get

          // Update the inter-task dependencies for the swap
          original.tasksDependingOnThisTask.foreach(t => {
            t.removeDependency(original)
            replacement ==> t
          })
          original.tasksDependedOn.foreach(t => {
            original.removeDependency(t)
            t ==> replacement
          })

          // update the task id map
          tasksToIds.removeValue(originalTaskId)
          tasksToIds.add(replacement, originalTaskId)

          // update the task info
          taskInfo.task = replacement

          // update the graph node
          val node: GraphNode = nodesToIds.keyFor(originalTaskId).get
          node.task = replacement

          // reset to no predecessors and ready for execution
          if (!isTaskDone(taskInfo.status)) {
            if (List(GraphNodeState.RUNNING, GraphNodeState.NO_PREDECESSORS, GraphNodeState.ONLY_PREDECESSORS).contains(node.state)) {
              node.state = GraphNodeState.PREDECESSORS_AND_UNEXPANDED
            }
            taskInfo.status = TaskStatus.UNKNOWN
          }

          // update the task <-> node
          tasksToNodes.removeValue(node)
          tasksToNodes.add(replacement, node)

          true
        }
    }
  }

  /** Resubmit a task for execution.  This will stop the task if it is currently running, and queue
    * it up for execution.  The number of attempts will be reset to zero.
    *
    * @param task the task to resubmit.
    * @return true if the task was successfully resubmitted, false otherwise.
    */
  def resubmitTask(task: Task): Boolean

  /** Checks if we have failed tasks.
   *
   * @return true if we have failed tasks, false otherwise.
   */
  def hasFailedTasks: Boolean = taskInfosToIds.keys.exists(info => TaskStatus.isTaskFailed(info.status))

  /** Returns true if all the predecessors of this task are known (have been added), false otherwise. */
  protected def allPredecessorsAdded(task: Task): Boolean = {
    predecessorsOf(task = task).nonEmpty
  }

  /** Gets the predecessor graph nodes of the given task in the execution graph.
   *
   * This assumes that all predecessors of this task have been added with addTasks and not removed.
   *
   * @param task the task to lookup.
   * @return the list of precedessors, or Nil if this task has no predecessors.  If there were predecessors that
    *         are not currently being tracked, None will be returned instead.
    */
  protected def predecessorsOf(task: Task): Option[Traversable[GraphNode]] = {
    task.tasksDependedOn match {
      case Nil => Some(Nil)
      case _ =>
        val predecessors: Traversable[Option[GraphNode]] = for (dependency <- task.tasksDependedOn) yield graphNodeFor(dependency)
        if (predecessors.exists(_.isEmpty)) None // we found predecessors that have no associated graph node
        else Some(predecessors.map(_.get))
    }
  }

  /** Gets the graph nodes in a given state.
   *
   * @param state the state of the returned graph nodes.
   * @return the graph nodes with the given state.
   */
  protected def graphNodesInStateFor(state: GraphNodeState.Value): Iterable[GraphNode] = {
    graphNodes.filter(n => n.state == state)
  }

  /** Gets the graph nodes in the given states.
    *
    * @param states the states of the returned graph nodes.
    * @return the graph nodes with the given states.
    */
  protected def graphNodesInStatesFor(states: Traversable[GraphNodeState.Value]): Iterable[GraphNode] = {
    graphNodes.filter(n => states.toList.contains(n.state))
  }

  /** Gets the graph nodes that imply they have predecessors.
    *
    * @return the graph nodes that have predecessors (unmet dependencies).
    */
  protected def graphNodesWithPredecessors: Iterable[GraphNode] = {
    graphNodes.filter(node => GraphNodeState.hasPredecessors(node.state))
  }
}
