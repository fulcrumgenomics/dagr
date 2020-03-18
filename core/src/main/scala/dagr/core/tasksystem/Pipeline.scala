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

import com.fulcrumgenomics.commons.io.Io
import com.fulcrumgenomics.commons.util.LazyLogging

import scala.collection.mutable

/** Simple trait to track tasks within a pipeline */
abstract class Pipeline(val outputDirectory: Option[Path] = None,
                        private var prefix: Option[String] = None,
                        private var suffix: Option[String] = None
                       ) extends Task with LazyLogging {
  private val tasks = new scala.collection.mutable.LinkedHashSet[Task]()
  this.outputDirectory.foreach(Io.mkdirs)

  /** A name exposed to sub-classes that can be treated like a Task, to add root tasks. */
  val root = new Dependable {
    /** Must be implemented to handle the addition of a dependent. */
    override def addDependent(dependent: Dependable): Unit = dependent.headTasks.foreach(tasks.add)

    /** Breaks the dependency link between this dependable and the provided Task. */
    override def !=> (other: Dependable): Unit = other.headTasks.foreach(tasks.remove)

    override def headTasks: Iterable[Task] = Pipeline.this.headTasks
    override def tailTasks: Iterable[Task] = Pipeline.this.tailTasks
    override def allTasks: Iterable[Task]  = Pipeline.this.allTasks
  }

  /**
    * Build your pipeline here. Use `==>` to define dependencies between tasks and use
    * `root ==> myTask` to capture tasks that have no dependencies and can start immediately.
    */
  def build(): Unit

  /** Will call build() and get the tasks.
    *
    * @return the list of tasks of to run.
    */
  final override def getTasks: Iterable[_ <: Task] = {
    build()
    tasks.toList.foreach(addChildren)

    // Do the necessary prefixing and suffixing of task names
    if (prefix.isDefined || suffix.isDefined) {
      tasks.foreach {
        case p: Pipeline =>
          p.prefix = if (p.prefix.isEmpty) prefix else p.prefix.map(prefix.getOrElse("") + _)
          p.suffix = if (p.suffix.isEmpty) suffix else p.suffix.map(suffix.getOrElse("") + _)
        case t: Task     => t.name = prefix.getOrElse("") + t.name + suffix.getOrElse("")
      }
    }

    tasks.toList
  }

  /** Recursively navigates dependencies, starting from the supplied task, and add all children to this.tasks. */
  private def addChildren(task : Task) : Unit = {
    // 1. find all tasks connected to this task
    val toVisit: mutable.Set[Task] = mutable.HashSet[Task](task)
    while (toVisit.nonEmpty) {
      val nextTask: Task = toVisit.head
      toVisit -= nextTask
      nextTask.tasksDependingOnThisTask.filterNot(tasks.contains).foreach { child =>
        tasks += child
        toVisit += child
      }
    }
  }

  /** True if we this pipeline is tracking this direct ancestor task, false otherwise. */
  def contains(task: Task): Boolean = tasks.contains(task)

  /** Builds an empty task for use within this pipeline. */
  def emptyTask: Task = Task.empty
}
