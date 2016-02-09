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

import java.nio.file.Path

import dagr.core.util.LazyLogging

/** Simple trait to track tasks within a pipeline */
abstract class Pipeline(val outputDirectory: Option[Path] = None) extends Task with LazyLogging {
  private val tasks = new scala.collection.mutable.LinkedHashSet[Task]()

  /** A name exposed to sub-classes that can be treated like a Task, to add root tasks. */
  val root = new Dependable {
    /** Causes the task to be added to the set of tasks for this Pipeline. */
    override def ==> (other: Task) : other.type = { addTask(other) }

    /** Causes all the task in the MultiTask to be added to the set of tasks for this Pipeline. */
    override def ==> (others: MultiTask) : MultiTask = { others.tasks.foreach(addTask(_)); others }

    /** Breaks the dependency link between this dependable and the provided Task. */
    override def !=>(other: Task): other.type = { tasks.remove(other); other }

    /** Breaks the dependency link between this dependable and the provided Tasks. */
    override def !=>(others: MultiTask): MultiTask = {others.tasks.foreach(tasks.remove) ; others}
  }

  /**
    * Override this and build your pipeline there. Use the 'add' method
    * to add tasks and make sure to define dependents between them.
    */
  def build(): Unit

  /** Will call build() and get the tasks.
    *
    * @return the list of tasks of to run.
    */
  final override def getTasks: Traversable[_ <: Task] = {
    build()
    tasks.toList.foreach(addChildren)
    tasks.toList
  }

  /** Recursively navigates dependencies, starting from the supplied task, and add all children to this.tasks. */
  private def addChildren(task : Task) : Unit = {
    tasks ++= task.tasksDependingOnThisTask
    task.tasksDependingOnThisTask.foreach(addChildren)
  }

  /** True if we this pipeline is tracking this direct ancestor task, false otherwise. */
  def contains(task: Task): Boolean = tasks.contains(task)

  /** Add one or more tasks to this pipeline. */
  def addTasks(task: Task*): Unit =  tasks ++= task

  /** Adds a single task, and returns the added task. */
  def addTask(task: Task): task.type = {
    tasks += task
    task
  }
}
