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

package dagr.core.exec

import com.fulcrumgenomics.commons.CommonsDef.{FilePath, unreachable}
import com.fulcrumgenomics.commons.io.Io
import dagr.core.reporting.ReplayLogger.{Definition, Status}
import dagr.core.reporting.ReportingDef.TaskRegister
import dagr.core.tasksystem.Task

import scala.collection.mutable

/** All implementations that decide if a given task should be executed should implement this. */
trait TaskCache extends TaskRegister {

  /** Returns true if the task should be executed, false otherwise. */
  def execute(task: Task): Boolean

  /** The method that will be called to registered the children of the parent.  If no children are given, then the task
    * is the root of all tasks.  If there is one child and it is equal to the parent, then it is a unit task and
    * should have already been registered, and so nothing is done.  Otherwise, the children are individually registered.
    * */
  def register(parent: Task, child: Task*): Unit = {
    if (isRoot(parent, child:_*)) {
      registerRoot(parent)
    }
    else if (isUnit(parent, child:_*)) {
      // It should have already been registered when its parent generated it!
      require(registered(parent), s"Task was not previously registered: $parent")
      if (this.taskSet.size == 1) { // special case: a root task that has no children
        checkStatusAndExecute(parent)
        // NB: need to check to see if there are children of the parent in the cache
        registerChildren(parent, Seq.empty)
      }
    }
    else {
      // ensure that we know about the parent task!
      require(registered(parent), s"Parent was not defined when registering its children: $parent")
      if (this.taskSet.size == 1) checkStatusAndExecute(parent) // special case: a root task that has no children
      registerChildren(parent, child)
    }
  }

  /** Returns the tasks that have been registered. For testing! */
  private[exec] def tasks: Seq[Task] = this.taskSet.toSeq

  /** Returns the tasks that have been registered. */
  protected def taskSet: Set[Task]

  /** Returns true if the task is a root task (has never depended on a task).*/
  protected def isRoot(parent: Task, child: Task*): Boolean = taskSet.isEmpty && child.isEmpty

  /** Returns true if the task does not generate any other tasks. */
  protected def isUnit(parent: Task, child: Task*): Boolean = child.length == 1 && child.forall(_ == parent)

  /** Returns true if this task builds other tasks. */
  protected def isBuilder(parent: Task, child: Task*): Boolean = !isRoot(parent, child:_*) && !isUnit(parent, child:_*)

  /** Returns true if the task has been registered, false otherwise. */
  protected def registered(task: Task): Boolean

  /** Registers the root task, namely a task that has no tasks (and never had any tasks) that depend on it. */
  protected def registerRoot(root: Task): Unit

  /** Registers the given children of the parent, assuming the children are the task built by the parent (and does not
    * include the parent itself) */
  protected def registerChildren(parent: Task, children: Seq[Task]): Unit

  /** Set the task to execute based on its status only. */
  protected def checkStatusAndExecute(task: Task): Unit
}

object TaskCache {
  def apply(replayLog: FilePath): TaskCache = SimpleTaskCache(replayLog)
}

object SimpleTaskCache {
  def apply(replayLog: FilePath): SimpleTaskCache = new SimpleTaskCache(Io.readLines(replayLog).toSeq, source=Some(replayLog.toString))
}

/** A very inefficient class to determine if tasks should be executed upon replay */
class SimpleTaskCache(replayLogLines: Seq[String], source: Option[String] = None) extends TaskCache {

  private val replayLog: String = source.getOrElse("Unknown")

  // TODO: are relationships even needed if we have both the definition of child, who has the parent code?
  /** Definitions, relationships, and statuses in the replay log */
  private val (definitions, statuses) = {
    val lines = replayLogLines
    val _definitions   = lines.filter(_.startsWith(Definition.Name)).map(Definition(_))
    val _statuses      = lines.filter(_.startsWith(Status.Name)).map(Status(_))
    (_definitions, _statuses)
  }

  // Validate various properties of the replay log
  {
    // Check that a task has one definition
    this.definitions.groupBy(d => d.code)
      .find { case (_, ds) => ds.length > 1 }
      .foreach { case (d, ds) =>
        throw new IllegalArgumentException(s"Replay log had ${ds.length} definitions for child '$d' in $replayLog")
    }
    // Check that a task if is found for each status
    this.statuses.map { s => s.definitionCode }
      .find { code => !this.definitions.exists(_.code == code) }
      .foreach { code =>
        val status = this.statuses.find(s => s.definitionCode == code).getOrElse {
          unreachable("A status should have been found")
        }
        throw new IllegalArgumentException(s"Replay log missing a definition for task '$code' with status '$status' in $replayLog")
      }
    // Check that the parent definition exists for all definitions except the root
    this.definitions.filterNot { d => Definition.isRootDefinition(d)}
      .foreach { d =>
        this.definitions.find { _.code == d.parentCode }.getOrElse {
          throw new IllegalArgumentException(s"Parent definition not found for child '$d' in $replayLog")
        }
      }
    // Check that a definition for a single root task is always found
    this.definitions.filter { d => Definition.isRootDefinition(d) } match {
      case Seq()  => throw new IllegalArgumentException(s"Did not find a definition for a root task in $replayLog")
      case Seq(r) => Unit // OK
      case defs   => throw new IllegalArgumentException(s"Found multiple definitions for a root task (${defs.map(_.code).mkString(", ")} in $replayLog")
    }
    // TODO: check that a root task was found
  }

  /** A mapping from a task to its current definition. */
  private val taskToDefinition: mutable.Map[Task, Definition] = new mutable.HashMap[Task, Definition]()

  /** A mapping from the current definition of a task to the definition defined in the replay log. */
  private val definitionMapping: mutable.Map[Definition, Definition] = new mutable.HashMap[Definition, Definition]()

  /** The set of tasks that should be executed. */
  private val tasksToExecute: mutable.Set[Task] = new mutable.HashSet[Task]()

  /** The set of tasks whose sub-tree should always be executed, since the task or an ancestor of the task could not be
    * mapped to a definition in the replay log. */
  private val missingAncestors: mutable.Set[Task] = new mutable.HashSet[Task]()

  /** Returns true if the task should be executed, false otherwise. */
  def execute(task: Task): Boolean = this.tasksToExecute.contains(task)

  /** Returns the tasks that have been registered. */
  protected def taskSet: Set[Task] = this.taskToDefinition.keySet.toSet

  /** Returns true if the task has been registered, false otherwise. */
  protected def registered(task: Task): Boolean = this.taskToDefinition.contains(task)

  /** Registers the root task, namely a task that has no tasks (and never had any tasks) that depend on it. */
  protected def registerRoot(root: Task): Unit = {
    // find the status in the replay log and decide what to do.
    val rootDefinition = Definition.buildRootDefinition(root)
    this.taskToDefinition(root) = rootDefinition
    // NB: the root always has the same parent code!
    // NB: assumes only one root
    this.definitions.find { d => d.parentCode == rootDefinition.parentCode } match {
      case None                   => unreachable(s"Could not find a root task in $replayLog")
      case Some(replayDefinition) => // make a decision
        this.definitionMapping(rootDefinition) = replayDefinition
        // NB: don't determine if it should be executed yet; register should be called again either with the root's
        // children, or root itself as its own child (a unit task).
    }
  }

  /** Registers the given children of the parent, assuming the children are the task built by the parent (and does not
    * include the parent itself) */
  protected def registerChildren(parent: Task, currentChildren: Seq[Task]): Unit = {
    val parentDefinition = currentDefinition(parent)

    // Create definitions for reach child
    val currentChildDefinitions = currentChildren.zipWithIndex.map { case (child, childNumber) =>
      buildDefinition(child, parentDefinition.code, childNumber)
    }.toIndexedSeq

    // Always execute the children if the parent has a missing ancestor (or is the missing ancestor itself).
    if (hasMissingAncestor(parent)) {
      setMissingAncestorAndExecute(currentChildren)
    }
    else {
      // Get the children of the parent from the previous execution
      val previousChildDefinitions = this.previousChildDefinitions(parent)

      // Check to see if any of the current child definitions are equivalent
      val equivalentChildren = currentChildDefinitions.exists { left =>
        currentChildDefinitions.exists { right => left != right && left.equivalent(right) }
      }

      // Check if we find the same # of children in the cache and none of the children are equivalent then test each
      // child individually
      if (previousChildDefinitions.length == currentChildren.length && !equivalentChildren) {
        // For each child, find the potential definitions from the previous execution
        val potentialDefinitions = currentChildDefinitions.map { currentChildDefinition =>
          // Find the definitions from the previous execution that could match the given definition from the current execution
          previousChildDefinitions.filter { previousChildDefinition =>
            previousChildDefinition.equivalent(currentChildDefinition)
          }
        }
        // If we have a single definition for each child, then we will decide whether or not to execute individually.
        if (potentialDefinitions.forall(_.length == 1)) {
          currentChildren.zip(potentialDefinitions.map(_.head))
            .foreach { case (child, previousChildDefinition) =>
              maybeSetTaskToExecute(child, previousChildDefinition)
            }
        }
        else {
          // NB: we assume all must be executed (conservatively)
          setMissingAncestorAndExecute(parent +: currentChildren)
        }
      }
      else {
        // NB: we assume all must be executed (conservatively)
        setMissingAncestorAndExecute(parent +: currentChildren)
      }
    }
  }

  /** Set the task to execute. */
  protected def setToExecute(task: Task): Unit = this.tasksToExecute += task

  /** Set the task to execute based on its status only. */
  protected def checkStatusAndExecute(task: Task): Unit = maybeSetTaskToExecute(task, previousDefinition(task))

  /** Returns true if the task already has a definition, false otherwise. */
  private def defined(definition: Definition): Boolean = this.definitionMapping.contains(definition)

  /** Returns the definition of the task from the current execution. */
  private def currentDefinition(task: Task): Definition = this.taskToDefinition(task)

  /** Returns the definition of the task from the previous execution. */
  private def previousDefinition(current: Definition): Definition = this.definitionMapping(current)

  /** Returns the definition of the task from the previous execution. */
  private def previousDefinition(task: Task): Definition = previousDefinition(currentDefinition(task))

  /** Checks if a task has a ancestor (parent) that was not defined in the previous execution. */
  private def hasMissingAncestor(task: Task): Boolean = this.missingAncestors.contains(task)

  /** Marks the task as having a missing ancestor. */
  private def setMissingAncestor(task: Task): Unit = this.missingAncestors += task

  /** Set the tasks to execute and have missing ancestors. */
  private def setMissingAncestorAndExecute(tasks: Seq[Task]): Unit = {
    tasks.foreach { task =>
      setMissingAncestor(task)
      setToExecute(task)
    }
  }

  /** Returns the definitions for all children of the parent from the previous execution */
  private def previousChildDefinitions(parent: Task): Seq[Definition] = {
    val previousParentDefinition = previousDefinition(parent)
    this.definitions.filter(_.parentCode == previousParentDefinition.code)
  }

  /** Checks the status of the task in the previous execution, and if not successful, sets it to execute in this execution. */
  private def maybeSetTaskToExecute(task: Task, previousDefinition: Definition): Unit = {
    require(task._executor.isDefined, s"Executor not defined for task '${task.name}'")
    this.statuses.filter { status => status.definitionCode == previousDefinition.code }
      .sortBy(-_.statusOrdinal)
      .map(status => task._executor.map(_.statusFrom(status.statusOrdinal).success).getOrElse(unreachable(s"Executor not set for task '${task.name}'")))
      .headOption match {
      case Some(s) if s => Unit // if it previously succeeded, don't set it to execute!
      case _            => setToExecute(task)
    }
  }

  /** Builds the definition and ensures that has not already been defined. */
  private def buildDefinition(task: Task, parentCode: Int, childNumber: Int): Definition = {
    val definition = Definition(task, parentCode, childNumber)
    require(!registered(task), s"Task was previously defined: $definition")
    require(!defined(definition), s"Task was previously defined: $definition")
    this.taskToDefinition(task) = definition
    definition
  }
}
