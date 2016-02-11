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
import java.sql.Timestamp

import dagr.DagrDef._
import dagr.core.execsystem.TaskStatus._
import dagr.core.execsystem.TaskManagerLike.BaseInfoAndNode
import dagr.core.execsystem.TaskTracker.InfoAndNode
import dagr.core.tasksystem.Task
import dagr.core.util.{BiMap, LazyLogging}

import scala.collection.mutable

object TaskTracker {
  case class InfoAndNode(info: TaskExecutionInfo, node: GraphNode) extends BaseInfoAndNode(info=info, node=node)
}

/** Tracks tasks during their execution */
trait TaskTracker extends TaskManagerLike with LazyLogging {

  /** Stores information about the execution of the current task and its place in the execution graph.  Mainly used
    * for convenience during lookups.
    */
  private final class TrackingInfo(task: Task, enclosingNode: Option[GraphNode] = None) {
    val (info: TaskExecutionInfo, node: GraphNode) = {
      val id: TaskId = task._id.getOrElse(unreachable(s"Task id not found for task with name '${task.name}'"))
      val submissionDate: Option[Timestamp] = Some(new Timestamp(System.currentTimeMillis))
      val info = new TaskExecutionInfo(
        task=task,
        status=UNKNOWN,
        script=scriptPathFor(task=task, id=id),
        logFile=logPathFor(task=task, id=id),
        submissionDate=submissionDate
      )
      val node = predecessorsOf(task = task) match {
        case None => new GraphNode(task=task, predecessorNodes=Nil, state=GraphNodeState.ORPHAN, enclosingNode=enclosingNode)
        case Some(predecessors) => new GraphNode(task=task, predecessorNodes=predecessors, enclosingNode=enclosingNode)
      }
      (info, node)
    }
  }

  /** The next id for a task */
  private var nextId: TaskId = 0

  /** The bi-directional map that stores the relationship between a task id and the task.  Needed to find a task
    * given its id, since a task knows about its id, but an id does not know about its task. */
  private val idLookup = mutable.Map[TaskId, Task]()

  /** The bi-directional map that stores the relationship between a task and its execution info. */
  private val taskAndInfoLookup = new BiMap[Task, TrackingInfo]()

  override def addTask(task: Task): TaskId = {
    addTask(task=task, enclosingNode=None, ignoreExists=false)
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
  protected[execsystem] def addTask(task: Task, enclosingNode: Option[GraphNode], ignoreExists: Boolean = false): TaskId = {
    // Make sure the id we will assign the task are not being tracked.
    if (idLookup.contains(nextId)) throw new IllegalArgumentException(s"Task '${task.name}' with id '$nextId' was already added!")

    taskIdFor(task) match {
      case Some(id) if ignoreExists => id
      case Some(id) => throw new IllegalArgumentException(s"Task '${task.name}' with id '$id' was already added!")
      case None =>
        // check for cycles
        checkForCycles(task = task)

        // set the task id
        val id = nextId
        task._id = Some(id)
        nextId += 1

        // update the lookups
        idLookup.put(id, task)
        taskAndInfoLookup.add(task, new TrackingInfo(task=task, enclosingNode=enclosingNode))

        id
    }
  }

  /** Adds tasks to be managed
    *
    * @param tasks the given tasks.
    * @param ignoreExists true if we just return the task id for already added tasks, false if we are to throw an [[IllegalArgumentException]]
    * @return the task identifiers.
    */
  protected[execsystem] def addTasks(tasks: Traversable[Task], enclosingNode: Option[GraphNode] = None, ignoreExists: Boolean = false): List[TaskId] = {
    tasks.map(task => addTask(task=task, enclosingNode=enclosingNode, ignoreExists=ignoreExists)).toList
  }

  override def addTasks(tasks: Task*): Seq[TaskId] = {
    tasks.map(task => addTask(task, enclosingNode=None, ignoreExists=false))
  }

  override def taskFor(id: TaskId): Option[Task] = idLookup.get(id)

  override def taskExecutionInfoFor(id: TaskId): Option[TaskExecutionInfo] = idLookup.get(id).flatMap(task => taskAndInfoLookup.valueFor(task)).map(_.info)

  override def taskExecutionInfoFor(task: Task): Option[TaskExecutionInfo] = taskAndInfoLookup.valueFor(task).map(_.info)

  /** Get the task's execution information if it exists.
    *
    * @param node the node associated with a task.
    * @return the task execution information if the task is managed, None otherwise
    */
  def taskExecutionInfoFor(node: GraphNode): Option[TaskExecutionInfo] = taskAndInfoLookup.valueFor(node.task).map(_.info)

  override def taskStatusFor(task: Task): Option[TaskStatus.Value] = taskAndInfoLookup.valueFor(task).map(_.info.status)

  override def taskStatusFor(id: TaskId): Option[TaskStatus.Value] = idLookup.get(id).flatMap(task => taskAndInfoLookup.valueFor(task)).map(_.info.status)

  override def taskIdFor(task: Task): Option[TaskId] = task._id

  override def taskIds(): Iterable[TaskId] = idLookup.keys

  /** Get the graph node associated with the task identifier
    *
    * @param id the task identifier
    * @return the graph node associated with the task identifier if found, None otherwise
    */
  def graphNodeFor(id: TaskId): Option[GraphNode] = idLookup.get(id).flatMap(task => taskAndInfoLookup.valueFor(task)).map(_.node)

  /** Get the graph node associated with the task
    *
    * @param task a task in the graph
    * @return the graph node associated with the task if found, None otherwise
    */
  def graphNodeFor(task: Task): Option[GraphNode] = taskAndInfoLookup.valueFor(task).map(_.node)

  /** Get the execution state of the task.
    *
    * @param id the task identifier.
    * @return the execution state of the task.
    */
  def graphNodeStateFor(id: TaskId): Option[GraphNodeState.Value] =  idLookup.get(id).flatMap(task => taskAndInfoLookup.valueFor(task)).map(_.node.state)

  /** Get the execution state of the task.
    *
    * @param task the task.
    * @return the execution state of the task.
    */
  def graphNodeStateFor(task: Task): Option[GraphNodeState.Value] = taskAndInfoLookup.valueFor(task).map(_.node.state)

  /** Get the graph nodes in the execution graph.
    *
    * @return the graph nodes, in no particular order.
    */
  def graphNodes: Iterable[GraphNode] = taskAndInfoLookup.values.map(_.node)

  override def apply(id: TaskId): Task = {
    this.taskFor(id=id) match {
      case Some(task) => task
      case None => throw new NoSuchElementException(s"key not found: $id")
    }
  }

  override def infoFor(task: Task): InfoAndNode = {
    this.taskAndInfoLookup.valueFor(key=task) match {
      case Some(trackingInfo) => InfoAndNode(trackingInfo.info, trackingInfo.node)
      case None => throw new NoSuchElementException(s"key not found: ${task.name}")
    }
  }

  override def infoFor(id: TaskId): InfoAndNode = {
    idLookup.get(id).flatMap(task => taskAndInfoLookup.valueFor(task)) match {
      case Some(trackingInfo) => InfoAndNode(trackingInfo.info, trackingInfo.node)
      case None => throw new NoSuchElementException(s"key not found: $id")
    }
  }

  override def taskToInfoBiMapFor: BiMap[Task, TaskExecutionInfo] = {
    val map: BiMap[Task, TaskExecutionInfo] = new BiMap[Task, TaskExecutionInfo]()
    for ((task, trackingInfo) <- taskAndInfoLookup) {
      map.add(task, trackingInfo.info)
    }
    map
  }

  override def replaceTask(original: Task, replacement: Task): Boolean = {
    // TODO: how to replace UnitTask vs. Workflows

    taskAndInfoLookup.valueFor(original) match {
      case None => false
      case Some(trackingInfo) =>
        if (replacement.tasksDependedOn.nonEmpty || replacement.tasksDependingOnThisTask.nonEmpty) false
        else {
          val id   = original._id.getOrElse(unreachable(s"Task id should be defined for task '${original.name}'"))
          val info = trackingInfo.info
          val node = trackingInfo.node

          // Update the inter-task dependencies for the swap
          original.tasksDependingOnThisTask.foreach(t => {
            t.removeDependency(original)
            replacement ==> t
          })
          original.tasksDependedOn.foreach(t => {
            original.removeDependency(t)
            t ==> replacement
          })

          // Update the id for both tasks
          replacement._id = original._id
          original._id = None

          // Update the tracking info
          trackingInfo.info.task = replacement
          trackingInfo.node.task = replacement

          // Replace original with replacement in the id lookup and task-and-info lookup
          idLookup.put(id, replacement)
          taskAndInfoLookup.removeKey(original)
          taskAndInfoLookup.add(replacement, trackingInfo)

          // reset to no predecessors and ready for execution
          if (!isTaskDone(info.status)) {
            if (List(GraphNodeState.RUNNING, GraphNodeState.NO_PREDECESSORS, GraphNodeState.ONLY_PREDECESSORS).contains(node.state)) {
              node.state = GraphNodeState.PREDECESSORS_AND_UNEXPANDED
            }
            info.status = TaskStatus.UNKNOWN
          }

          true
        }
    }
  }

  // Turning it off until `resubmit` is used.
  //def resubmitTask(task: Task): Boolean

  override def hasFailedTasks: Boolean = taskAndInfoLookup.values.exists(trackingInfo => TaskStatus.isTaskFailed(trackingInfo.info.status))

  /** Generates a path to a file to hold the task command */
  protected def scriptPathFor(task: Task, id: TaskId): Path

  /** Generates a path to file to store the log output of the task */
  protected def logPathFor(task: Task, id: TaskId): Path

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
