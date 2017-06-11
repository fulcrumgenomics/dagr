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
import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.reporting.ExecutionLogger.{Definition, Relationship, Status}
import dagr.core.reporting.ReportingDef.TaskRegister
import dagr.core.tasksystem.Task

import scala.collection.mutable

/** All implementations that decide if a given task should be executed should implement this. */
trait TaskCache extends TaskRegister {
  /** Returns true if the task should be executed, false otherwise. */
  def execute(task: Task): Boolean
}

object TaskCache {
  def apply(replayLog: FilePath): TaskCache = new SimpleTaskCache(replayLog)
}

/** A very inefficient class to determine if tasks should be executed upon replay */
class SimpleTaskCache(replayLog: FilePath) extends TaskCache with LazyLogging {

  /** Definitions, relationships, and statuses in the replay log */
  private val (definitions, relationships, statuses) = {
    val lines = Io.readLines(replayLog).toSeq
    val _definitions   = lines.filter(_.startsWith(Definition.Name)).map(Definition(_))
    val _relationships = lines.filter(_.startsWith(Relationship.Name)).map(Relationship(_))
    val _statuses      = lines.filter(_.startsWith(Status.Name)).map(Status(_))
    (_definitions, _relationships, _statuses)
  }

  /** A mapping from a task to its current definition. */
  private val taskToDefinition: mutable.Map[Task, Definition] = new mutable.HashMap[Task, Definition]()

  /** A mapping from the current definition of a task to the definition defined in the replay log. */
  private val definitionMapping: mutable.Map[Definition, Definition] = new mutable.HashMap[Definition, Definition]()

  /** The set of tasks that should be executed. */
  private val tasksToExecute: mutable.Set[Task] = new mutable.HashSet[Task]()

  /** The set of tasks whose sub-tree should always be executed, since the tasks could not be mapped to a definition in
    * the replay log. */
  private val tasksForcedToExecute: mutable.Set[Task] = new mutable.HashSet[Task]()

  /** Returns true if the task should be executed, false otherwise. */
  def execute(task: Task): Boolean = {
    logger.debug(s"assessing task '${task.name}': " + (if (tasksToExecute.contains(task)) "execute" else "skip"))
    tasksToExecute.contains(task)
  }

  /** The method that will be called on the result of `Task.getTasks`. */
  def register(parent: Task, child: Task*): Unit = {
    if (child.forall(_ == parent)) {
      // If the task is the root task (has no parents), taskToDefinition should be empty.
      if (this.taskToDefinition.isEmpty) { // root task
        // find the status in the replay log and decide what to do.
        val rootDefinition = Definition.buildRootDefinition(parent)
        this.taskToDefinition(parent) = rootDefinition
        // NB: the root always has the same parent code!
        // NB: assumes only one root
        this.definitions.find { d => d.parentCode == rootDefinition.parentCode } match {
          case None                   =>
            throw new IllegalArgumentException(s"Could not find a root task in $replayLog")
          case Some(replayDefinition) => // make a decision
            require(replayDefinition.childNumber == rootDefinition.childNumber,
              s"Expected child number '${rootDefinition.childNumber}' for root task with definition: '$replayDefinition'")
            logger.debug(s"Root task found: $replayDefinition")
            this.definitionMapping(rootDefinition) = replayDefinition
            maybeSetTaskToExecute(parent, replayDefinition)
        }
      }
      else { // unit task
        // Otherwise, it should have already been registered as child task of a parent!
        require(this.taskToDefinition.contains(parent), s"Task was not previously registered: $parent")
      }
    }
    else {
      // ensure that we know about the parent task!
      require(this.taskToDefinition.contains(parent), s"Parent was not defined when registering its children: $parent")

      // check that we have the same # of children
      val parentDefinition       = this.taskToDefinition(parent)
      val parentReplayDefinition = this.definitionMapping(parentDefinition)

      // Require the same # of children and that they all have the same simple names (in order)
      val validChildren = {
        val numChildren = this.relationships.count(_.parentCode == parentReplayDefinition.code)
        val simpleNames = this.relationships.filter(_.parentCode == parentReplayDefinition.code).map { relationship =>
          val childDefinition = this.definitions.find {
            _.code == relationship.childCode
          }.getOrElse {
            throw new IllegalArgumentException(s"No definition for child in relationship: $relationship")
          }
          childDefinition.simpleName
        }
        val sameSimpleNames = child.map(Definition.getSimpleName).zip(simpleNames).forall { case (a, b) => a == b }
        logger.debug(s"Given ${child.length} children and found $numChildren children in the replay")
        logger.debug(s"Found the same simple names? $sameSimpleNames")
        numChildren == child.length && sameSimpleNames
      }

      if (validChildren) {
        // for each child task, try to associate it with a know task
        child.zipWithIndex.foreach { case (_child, childNumber) =>
          addExecutionDecision(parent, _child, childNumber)
        }
      }
      else {
        // a whole new set of children, execute them
        child.zipWithIndex.foreach { case (_child, childNumber) =>
          logger.debug(s"Adding task ${_child.name} to execute due to differing child number")
          buildDefinition(_child, parentDefinition.code, childNumber)
          this.tasksToExecute += _child
          this.tasksForcedToExecute += _child
        }
      }
    }
    child.foreach { task =>
      logger.debug(s"registered task '${task.name}': " + (if (tasksToExecute.contains(task)) "execute" else "skip"))
    }
  }

  /** Builds the definition and ensures that has not already been defined. */
  private def buildDefinition(task: Task, parentCode: Int, childNumber: Int): Definition = {
    val definition = Definition(task, parentCode, childNumber)
    require(!this.taskToDefinition.contains(task), s"Task was previously defined: $definition")
    require(!this.definitionMapping.contains(definition),  s"Task was previously defined: $definition")
    this.taskToDefinition(task) = definition
    definition
  }

  /** Updates tasksToExecute if the task should be executed. */
  private def maybeSetTaskToExecute(task: Task, taskReplayDefinition: Definition): Unit = {
    require(task._executor.isDefined, s"Executor not defined for task '${task.name}'")
    val succeededExecution = this.statuses.filter { status => status.definitionCode == taskReplayDefinition.code }
      .sortBy(-_.statusOrdinal)
      .map(status => task._executor.map(_.from(status.statusOrdinal).success).getOrElse(unreachable(s"Executor not set for task '${task.name}'")))
      .headOption
      .getOrElse(false)

    if (!succeededExecution) {
      // execute it
      logger.debug(s"Adding task ${task.name} to execute")
      this.tasksToExecute += task
    }
  }

  private def addExecutionDecision(parent: Task, child: Task, childNumber: Int): Unit = {
    val parentDefinition = this.taskToDefinition(parent)
    val childDefinition  = buildDefinition(child, parentDefinition.code, childNumber)

    // Check if the parent was forced to execute, due to not being in the replay log.
    if (this.tasksForcedToExecute.contains(parent)) {
      logger.debug(s"Forcing child ${child.name} to execute due to parent")
      this.tasksToExecute += child
      this.tasksForcedToExecute += child
    }
    else {
      // NB: this does not exist if the parent was forced to execute!
      val parentReplayDefinition = this.definitionMapping(parentDefinition)
      // find the definition of the child in the replay log
      val childReplayDefinitions = this.relationships
        .filter { relationship => relationship.parentCode == parentReplayDefinition.code } // find all potential relationships of its parent to a task
        .flatMap { relationship =>
          logger.debug(s"Examining relationship $relationship")
          // find the definition of the child task in this relationship
          val definition = this.definitions.find(_.code == relationship.childCode).getOrElse {
            throw new IllegalArgumentException(s"No definition for child in relationship: $relationship")
          }
          logger.debug(s"Found definition '$definition' expecting '$childDefinition'")
          // child number
          if (definition.equivalent(childDefinition)) {
            Some(definition)
          }
          else {
            None
          }
        }

      // there can be only one!
      childReplayDefinitions match {
        case Seq(childReplayDefinition) =>
          this.definitionMapping(childDefinition) = childReplayDefinition
          maybeSetTaskToExecute(child, childReplayDefinition)
        case defs => None // either no relationships or more than one relationship!
          logger.debug(s"Forcing child ${child.name} to execute since ${defs.length} child definition(s) found!")
          this.tasksToExecute += child
          this.tasksForcedToExecute += child
      }
    }
  }
}