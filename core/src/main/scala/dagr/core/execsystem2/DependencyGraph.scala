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

package dagr.core.execsystem2

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.exec.ExecDef
import dagr.core.tasksystem.Task

/**
  * A trait that classes that track and update dependencies on tasks should implement.
  */
trait DependencyGraph {

  /** Add a task to the dependency graph and returns true if the task has no dependencies, false otherwise.  */
  def add(task: Task): Boolean

  /** None if the task was already added, true if the task was added and has no dependencies, false otherwise. */
  def maybeAdd(task: Task): Option[Boolean]

  /** Removes this task as a dependency for all other tasks in this dependency graph.  The task should not depend on
    * any tasks, and all tasks that depend on it will have their dependency on this task removed.
    */
  def remove(task: Task): Seq[Task]

  /** Returns None if the task is not in the graph, true if it has dependencies, false otherwise.
    */
  def hasDependencies(task: Task): Option[Boolean]

  /** Returns true if the task is in the graph, false otherwise. */
  def contains(task: Task): Boolean

  /** The number of tasks in the dependency graph. */
  def size: Int

  /** Throws an exception if there is a cycle in the dependency graph.  The exception may have relevant debug
    * information.
    * @param task
    */
  def exceptIfCyclicalDependency(task: Task): Unit
}

object DependencyGraph {
  /** Returns a default implementation of a dependency graph (i.e. a [[SimpleDependencyGraph]])*/
  def apply(): DependencyGraph = new SimpleDependencyGraph
}

/**
  * A very simple dependency graph that uses a [[CountDownLatch]] on the number of dependencies for a [[Task]] to
  * block until a task has no dependencies.
  */
private class SimpleDependencyGraph extends DependencyGraph with LazyLogging {
  import scala.collection.mutable

  /** The map of tasks to the number of remaining unsatisfied dependencies. */
  private val graph: mutable.Map[Task, AtomicInteger] = ExecDef.concurrentMap()

  /** Adds the task to the dependency graph.  Returns None if the task has already been added, true if the task
    * has no dependencies, and false if it has dependencies. */
  def maybeAdd(task: Task): Option[Boolean] = this.synchronized { if (contains(task)) None else Some(add(task)) }

  /** Adds the task to the dependency graph.  The task should not already be part of the graph. Returns true if the task
    * has no dependencies, and false if it has dependencies */
  def add(task: Task): Boolean = this.synchronized {
    require(!this.graph.contains(task), s"Task '${task.name}' is already part of the dependency graph")
    this.graph.put(task, new AtomicInteger(task.tasksDependedOn.size))
    !this.hasDependencies(task).get
  }

  /** Removes this task from the dependency graph.  It should not depend on any tasks itself, and all tasks that depend
    * on it will have their dependency on this task removed.  Returns any dependent task that now has no more
    * dependencies.
    */
  def remove(task: Task): Seq[Task] = {
    require(task.tasksDependedOn.isEmpty,
      s"Removing a task '${task.name}' from the dependency graph that has dependencies: "
        + task.tasksDependedOn.map(_.name).mkString(", "))
    // remove this as a dependency for all other tasks that depend on this task
    task.tasksDependingOnThisTask.flatMap { dependent =>
      dependent.synchronized {
        require(this.graph.contains(dependent), s"Dependent '${dependent.name}' not in the dependency graph")
        task !=> dependent
        val latch = this.graph(dependent)
        if (latch.decrementAndGet() == 0) Some(dependent) else None
      }
    }.toSeq
  }

  /** Returns None if the task is not part of the graph, true if the task has dependencies, false otherwise. */
  def hasDependencies(task: Task): Option[Boolean] = {
    this.graph.get(task).map { e => e.get() > 0 }
  }

  /** Returns true if the task is part of the graph, false otherwise. */
  def contains(task: Task): Boolean = this.graph.contains(task)

  /** Returns the number of tasks in the graph. */
  def size: Int = this.graph.size

  // NB: I think that the dependents in Task could be updated while were are doing this!  How do we synchronize?  Do we
  // have a global lock in the Task object?
  /** Throws an exception if there is a cycle in the dependency graph.  The exception may have relevant debug
    * information.
    * @param task
    */
  def exceptIfCyclicalDependency(task: Task): Unit = this.synchronized {
    // check for cycles
    if (Task.hasCycle(task)) {
      logger.error("Task was part of a graph that had a cycle")
      for (component <- Task.findStronglyConnectedComponents(task = task)) {
        if (Task.isComponentACycle(component = component)) {
          logger.error("Tasks were part of a strongly connected component with a cycle: "
            + component.map(t => s"'${t.name}'").mkString(", "))
        }
      }
      throw new IllegalArgumentException(s"Task was part of a graph that had a cycle '${task.name}'")
    }
  }
}

