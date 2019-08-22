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
package dagr.core.tasksystem

import com.fulcrumgenomics.commons.CommonsDef.unreachable
import dagr.core.execsystem.TaskExecutionInfo

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

/** Utility methods to aid in working with a task. */
object Task {

  /** Marker trait for empty tasks. */
  trait EmptyTask extends Task

  /** A task that does nothing. */
  def empty: Task = new EmptyTask {
    name = "Task.empty"
    override def getTasks: Iterable[_ <: Task] = None
  }

  /** Helper class for Tarjan's strongly connected components algorithm */
  private class TarjanData {
    var index: Int = 0
    val stack: mutable.Stack[Task] = new mutable.Stack[Task]()
    val onStack: mutable.Set[Task] = new mutable.HashSet[Task]()
    val indexes: mutable.Map[Task, Int] = new mutable.HashMap[Task, Int]()
    val lowLink: mutable.Map[Task, Int] = new mutable.HashMap[Task, Int]()
    val components: mutable.Set[mutable.Set[Task]] = new mutable.HashSet[mutable.Set[Task]]()
  }

  /** Detects cycles in the DAG to which this task belongs.
    *
    * @param task the task to begin search.
    * @return true if the DAG to which this task belongs has a cycle, false otherwise.
    */
  private[core] def hasCycle(task: Task): Boolean = {
    findStronglyConnectedComponents(task).exists(component => isComponentACycle(component))
  }

  /** Finds all the strongly connected components of the graph to which this task is connected.
    *
    * Uses Tarjan's algorithm: https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm
    *
    * @param task a task in the graph to check.
    * @return the set of strongly connected components.
    */
  private[core] def findStronglyConnectedComponents(task: Task): Set[Set[Task]] = {

    // 1. find all tasks connected to this task
    val visited: mutable.Set[Task] = new mutable.HashSet[Task]()
    val toVisit: mutable.Set[Task] = mutable.HashSet[Task](task)

    while (toVisit.nonEmpty) {
      val nextTask: Task = toVisit.head
      toVisit -= nextTask
      (nextTask.tasksDependedOn.toList ::: nextTask.tasksDependingOnThisTask.toList).foreach(t => if (!visited.contains(t)) toVisit += t)
      visited += nextTask
    }

    // 2. Runs Tarjan's strongly connected components algorithm
    val data: TarjanData = new TarjanData
    visited.filterNot(data.indexes.contains).foreach(v => findStronglyConnectedComponent(v, data))

    // return all the components
    data.components.map(component => component.toSet).toSet
  }

  /** Indicates if a given set of tasks that are strongly connected components contains a cycle. This is the
    * case if the set size is greater than one, or the task is depends on itself.  See [[Task.findStronglyConnectedComponents()]]
    * for how to retrieve strongly connected components from a task.
    *
    * @param component the strongly connected component.
    * @return true if the component contains a cycle, false otherwise.
    */
  private[core] def isComponentACycle(component: Set[Task]): Boolean = {
    if (1 < component.size) true
    else {
      component.headOption match {
        case Some(task) =>
          task.tasksDependedOn.toSet.contains(task) ||
          task.tasksDependingOnThisTask.toSet.contains(task)
        case _ => false
      }
    }
  }

  /** See https://en.wikipedia.org/wiki/Tarjan%27s_strongly_connected_components_algorithm */
  private def findStronglyConnectedComponent(v: Task, data: TarjanData): Unit = {
    // Set the depth index for v to the smallest unused index
    data.indexes += Tuple2(v, data.index)
    data.lowLink += Tuple2(v, data.index)
    data.index += 1
    data.stack.push(v)
    data.onStack += v

    // Consider successors of v
    for(w <- v.tasksDependedOn) {  // could alternatively use task.getTasksDependingOnThisTask
      if (!data.indexes.contains(w)) {
        // Successor w has not yet been visited; recurse on it
        findStronglyConnectedComponent(w, data)
        data.lowLink.put(v, math.min(data.lowLink.get(v).get, data.lowLink.get(w).get))
      }
      else if (data.onStack(w)) {
        // Successor w is in stack S and hence in the current SCC
        data.lowLink.put(v, math.min(data.lowLink.get(v).get, data.lowLink.get(w).get))
      }
    }

    // If v is a root node, pop the stack and generate an SCC
    if (data.indexes.get(v).get == data.lowLink.get(v).get) {
      val component: mutable.Set[Task] = new mutable.HashSet[Task]()
      breakable {
        while (data.stack.nonEmpty) {
          val w: Task = data.stack.pop()
          data.onStack -= w
          component += w
          if (w == v) break
        }
      }
      data.components += component
    }
  }
}

/** Base class for all tasks, multi-tasks, and workflows.
 *
 * Once a task is constructed, it has the following evolution:
 * 1. Any tasks on which it depends are added (see [[==>]]).
 * 2. When all tasks on which it is dependent have completed, the [[getTasks]] method
 *    is called to create a set of tasks. This task becomes dependent on any task that
 *    is returned that is not itself.
 * 3. When all newly dependent tasks from #2 are complete, as well as this task, the
 *    [[onComplete]] method is called to perform any light-weight modification of this
 *    task.
 * 4. If a task failed during execution or within [[onComplete]], the [[retry]]
 *    method will be called until the task no longer wishes to retry or succeeds.
 */
trait Task extends Dependable {
  /** The unique id given to this task by the execution system, or None if not being tracked. */
  private[core] var _taskInfo : Option[TaskExecutionInfo] = None
  private[core] def taskInfo : TaskExecutionInfo = _taskInfo.getOrElse(unreachable(s"Task id should be defined for task '$name'"))

  /** The name of the task. */
  var name: String = try { getClass.getSimpleName } catch {
    case ex: java.lang.InternalError =>
      throw new RuntimeException("Is your class internal? See: https://github.com/scala/bug/issues/2034", ex)
  }

  /* The set of tasks that this task depends on. */
  private val dependsOnTasks    = new ListBuffer[Task]()
  /* The set of tasks that depend on this task. */
  private val dependedOnByTasks = new ListBuffer[Task]()

  /** Gets the sequence of tasks that this task depends on.. */
  protected[core] def tasksDependedOn: Iterable[Task] = this.dependsOnTasks.toList

  /** Gets the sequence of tasks that depend on this task. */
  protected[core] def tasksDependingOnThisTask: Iterable[Task] = this.dependedOnByTasks.toList

  /** Must be implemented to handle the addition of a dependent. */
  override def addDependent(dependent: Dependable): Unit = dependent.headTasks.foreach(t => {
      t.dependsOnTasks += this
      this.dependedOnByTasks += t
    })

  /** Removes this as a dependency for other */
  override def !=>(other: Dependable): Unit = other.headTasks.foreach(_.removeDependency(this))

  override def headTasks: Iterable[Task] = Seq(this)
  override def tailTasks: Iterable[Task] = Seq(this)
  override def allTasks: Iterable[Task]  = Seq(this)

  /**
     * Removes a dependency by removing the supplied task from the list of dependencies for this task
     * and removing this from the list of tasks depending on "task".
     *
     * @param task a task on which this task depends
     * @return true if a dependency existed and was removed, false otherwise
     */
  def removeDependency(task: Task): Boolean = {
    if (this.dependsOnTasks.contains(task)) {
      this.dependsOnTasks -= task
      task.dependedOnByTasks -= this
      true
    }
    else
      false
  }

  /** Sets the name of this task. */
  def withName(name: String) : this.type = { this.name = name; this }

  /** Get the list of tasks to execute.
   *
   * All tasks, multi-tasks, workflows, and other task-like-entities should implement this method.
   * In the execution graph, the returned tasks are all children, but not necessarily leaves,
   * meaning the returned tasks themselves may spawn tasks.  This task could also generate
   * a mutated or modified task different from this task.  It is perfectly reasonable for
   * tasks in the returned list to be themselves be interdependent, but they should not
   * be dependent on tasks not within this list.
   *
   * @return the list of tasks of to run.
   */
  def getTasks: Iterable[_ <: Task]

  /** Finalize anything after the task has been run.
   *
   * This method should be called after a task has been run. The intended use of this method
   * is to allow for any modification of this task prior to any dependent tasks being run.  This
   * would allow any parameters that were passed to dependent tasks as call-by-name to be
   * finalized here.  For example, we could have passed an Option[String] that is None
   * until make it Some(String) in this method.  Then when the dependent task's getTasks
   * method is called, it can call 'get' on the option and get something.
   *
   * @param exitCode the exit code of the task, which could also be 1 due to the system terminating this process
   * @return true if we c
   */
  def onComplete(exitCode: Int): Boolean = true
}
