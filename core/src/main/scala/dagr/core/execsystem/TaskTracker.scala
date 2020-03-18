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

import com.fulcrumgenomics.commons.CommonsDef._
import com.fulcrumgenomics.commons.collection.BiMap
import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.DagrDef._
import dagr.core.execsystem.TaskStatus._
import dagr.core.tasksystem.Task

import scala.collection.mutable

/** Tracks tasks during their execution */
trait TaskTracker extends TaskManagerLike with LazyLogging {

  /** Stores information about the execution of the current task and its place in the execution graph.  Mainly used
    * for convenience during lookups.
    */
  private final class TrackingInfo(task: Task, id: TaskId, enclosingNode: Option[GraphNode] = None) {
    val (info: TaskExecutionInfo, node: GraphNode) = {
      val info = new TaskExecutionInfo(
        task=task,
        taskId=id,
        status=UNKNOWN,
        script=scriptPathFor(task=task, id=id, attemptIndex=1),
        logFile=logPathFor(task=task, id=id, attemptIndex=1),
        submissionDate=Some(Instant.now())
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

  /** TaskId is unique, so use it a key to store tasks, as the hash code for a task or node could collide. */
  private val idToTask: mutable.Map[TaskId, Task] = mutable.Map[TaskId, Task]()
  private val idToNode: mutable.Map[TaskId, GraphNode] = mutable.Map[TaskId, GraphNode]()

  override def addTask(task: Task): TaskId = {
    addTask(task=task, enclosingNode=None, ignoreExists=false)
  }

  private def addTaskNoChecking(task: Task, enclosingNode: Option[GraphNode] = None): TaskId = {
    // set the task id
    val id = yieldAndThen(nextId) {nextId += 1}
    // set the task info
    require(task._taskInfo.isEmpty) // should not have any info!
    val info = new TaskExecutionInfo(
      task=task,
      taskId=id,
      status=UNKNOWN,
      script=scriptPathFor(task=task, id=id, attemptIndex=1),
      logFile=logPathFor(task=task, id=id, attemptIndex=1),
      submissionDate=Some(Instant.now())
    )
    task._taskInfo = Some(info)

    // create the graph node
    val node = predecessorsOf(task=task) match {
      case None => new GraphNode(task=task, predecessorNodes=Nil, state=GraphNodeState.ORPHAN, enclosingNode=enclosingNode)
      case Some(predecessors) => new GraphNode(task=task, predecessorNodes=predecessors, enclosingNode=enclosingNode)
    }

    // update the lookups
    idToTask.put(id, task)
    idToNode.put(id, node)

    id
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
    if (idToTask.contains(nextId)) throw new IllegalArgumentException(s"Task '${task.name}' with id '$nextId' was already added!")

    taskFor(task) match {
      case Some(id) if ignoreExists => id
      case Some(id) => throw new IllegalArgumentException(s"Task '${task.name}' with id '$id' was already added!")
      case None =>
        // check for cycles
        checkForCycles(task = task)
        // add the task
        addTaskNoChecking(task, enclosingNode)
    }
  }

  /** Adds tasks to be managed
    *
    * @param tasks the given tasks.
    * @param ignoreExists true if we just return the task id for already added tasks, false if we are to throw an [[IllegalArgumentException]]
    * @return the task identifiers.
    */
  protected[execsystem] def addTasks(tasks: Seq[Task], enclosingNode: Option[GraphNode] = None, ignoreExists: Boolean = false): Seq[TaskId] = {
    // Make sure the id we will assign the task are not being tracked.
    if (idToTask.contains(nextId)) throw new IllegalArgumentException(s"Task id '$nextId' was already added!")

    val tasksToAdd = tasks.flatMap { task =>
      taskFor(task) match {
        case Some(_) if ignoreExists => None
        case Some(id) => throw new IllegalArgumentException(s"Task '${task.name}' with id '$id' was already added!")
        case None     => Some(task)
      }
    }

    checkForCycles(tasksToAdd:_*)

    tasks.map { task => taskFor(task).getOrElse(addTaskNoChecking(task, enclosingNode)) }
  }

  override def addTasks(tasks: Task*): Seq[TaskId] = {
    this.addTasks(tasks, enclosingNode=None, ignoreExists=false)
  }

  override def taskFor(id: TaskId): Option[Task] = idToTask.get(id)

  override def taskExecutionInfoFor(id: TaskId): Option[TaskExecutionInfo] = idToNode.get(id).map(_.taskInfo)

  override def taskExecutionInfoFor(task: Task): Option[TaskExecutionInfo] = task._taskInfo

  override def taskStatusFor(task: Task): Option[TaskStatus.Value] = task._taskInfo.map(_.status)

  override def taskStatusFor(id: TaskId): Option[TaskStatus.Value] = idToTask.get(id).flatMap(taskStatusFor)

  override def taskFor(task: Task): Option[TaskId] = task._taskInfo.map(_.taskId)

  override def taskIds(): Iterable[TaskId] = idToTask.keys

  /** Get the graph node associated with the task identifier
    *
    * @param id the task identifier
    * @return the graph node associated with the task identifier if found, None otherwise
    */
  def graphNodeFor(id: TaskId): Option[GraphNode] = idToNode.get(id)

  /** Get the graph node associated with the task
    *
    * @param task a task in the graph
    * @return the graph node associated with the task if found, None otherwise
    */
  def graphNodeFor(task: Task): Option[GraphNode] = taskFor(task).flatMap(idToNode.get)

  /** Get the execution state of the task.
    *
    * @param id the task identifier.
    * @return the execution state of the task.
    */
  def graphNodeStateFor(id: TaskId): Option[GraphNodeState.Value] = graphNodeFor(id).map(_.state)


  /** Get the execution state of the task.
    *
    * @param task the task.
    * @return the execution state of the task.
    */
  def graphNodeStateFor(task: Task): Option[GraphNodeState.Value] = taskFor(task).flatMap(graphNodeStateFor)

  /** Get the graph nodes in the execution graph.
    *
    * @return the graph nodes, in no particular order.
    */
  def graphNodes: Iterable[GraphNode] = idToNode.values

  override def apply(id: TaskId): GraphNode = {
    this.graphNodeFor(id=id) match {
      case Some(node) => node
      case None => throw new NoSuchElementException(s"key not found: $id")
    }
  }

  override def taskToInfoBiMapFor: BiMap[Task, TaskExecutionInfo] = {
    val map: BiMap[Task, TaskExecutionInfo] = new BiMap[Task, TaskExecutionInfo]()
    idToTask.foreach { case (id, task) => map.add(task, task.taskInfo) }
    map
  }

  override def replaceTask(original: Task, replacement: Task): Boolean = {
    // TODO: how to replace UnitTask vs. Workflows

    graphNodeFor(original) match {
      case None => false
      case Some(node) =>
        if (replacement.tasksDependedOn.nonEmpty || replacement.tasksDependingOnThisTask.nonEmpty) false
        else {
          val info = original.taskInfo
          val id   = info.taskId

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
          replacement._taskInfo = original._taskInfo
          original._taskInfo = None

          // Update the task in the info and dode
          info.task = replacement
          node.task = replacement

          // Replace original with replacement in the id lookup and task-and-info lookup
          idToTask.put(id, replacement)
          idToNode.put(id, node)

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

  override def hasFailedTasks: Boolean = idToNode.values.exists(trackingInfo => TaskStatus.isTaskFailed(trackingInfo.taskInfo.status))

  /** Generates a path to a file to hold the task command */
  protected def scriptPathFor(task: Task, id: TaskId, attemptIndex: Int): Path

  /** Generates a path to file to store the log output of the task */
  protected def logPathFor(task: Task, id: TaskId, attemptIndex: Int): Path

  /** Checks for cycles in the graph to which the task belongs.
    *
    * Throws an [[IllegalArgumentException]] if a cycle was found after logging each strongly connected component with
    * a cycle in the graph.
    *
    * @param task a task in the graph to check.
    */
  protected def checkForCycles(task: Task*): Unit = {
    // check for cycles
    if (Task.hasCycle(task:_*)) {
      logger.error("Task was part of a graph that had a cycle")
      for (component <- Task.findStronglyConnectedComponents(task = task:_*)) {
        if (Task.isComponentACycle(component = component)) {
          logger.error("Tasks were part of a strongly connected component with a cycle: "
            + component.map(t => s"[${t.name}]").mkString(", "))
        }
      }
      throw new IllegalArgumentException(s"Task(s) had cyclical dependencies [${task.map(_.name).mkString(",")}]")
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
  protected def predecessorsOf(task: Task): Option[Iterable[GraphNode]] = {
    task.tasksDependedOn match {
      case Nil => Some(Nil)
      case _ =>
        val predecessors: Iterable[Option[GraphNode]] = for (dependency <- task.tasksDependedOn) yield graphNodeFor(dependency)
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
  protected def graphNodesInStatesFor(states: Iterable[GraphNodeState.Value]): Iterable[GraphNode] = {
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
