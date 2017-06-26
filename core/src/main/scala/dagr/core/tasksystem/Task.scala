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

import java.time.{Duration, Instant}

import com.fulcrumgenomics.commons.CommonsDef.{FilePath, unreachable}
import com.fulcrumgenomics.commons.util.Logger
import com.fulcrumgenomics.commons.util.TimeUtil.formatElapsedTime
import dagr.core.DagrDef.TaskId
import dagr.core.exec.{Executor, ResourceSet}
import dagr.core.execsystem.TaskExecutionInfo
import dagr.core.tasksystem.Task.TaskInfo

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.control.Breaks._

/** Utility methods to aid in working with a task. */
object Task {

  /** The status of a task.  Any execution system requiring a custom set of statuses should extend this trait. */
  trait TaskStatus {
    /** A brief description of the status. */
    def description: String
    /** A unique ordinal for the status, used to prioritize reporting of statuses*/
    def ordinal: Int
    /** The name of the status, by default the class' simple name (sanitized). */
    def name: String = this.getClass.getSimpleName.replaceFirst("[$].*$", "")
    /** Returns true if this status indicates any type of success, false otherwise. */
    def success: Boolean
    /** The string representation of the status, by default the definition. */
    override def toString: String = this.description
  }

  /** The execution information for a task.  Used for extrenal read-only access to [[TaskInfo]]. Any execution system
    * should extend [[TaskInfo]] instead class to store their specific metadata. */
  trait TaskInfoLike extends Ordered[TaskInfoLike] {
    def task       : Task
    def id         : Option[TaskId]
    def attempts   : Int
    def script     : Option[FilePath]
    def log        : Option[FilePath]
    def resources  : Option[ResourceSet]
    def exitCode   : Option[Int]
    def throwable  : Option[Throwable]

    /** The current status of the task. */
    def status     : TaskStatus

    /** The instant the task reached a given status. */
    def timePoints :  Traversable[TimePoint]

    /** The instant the task reached the current status. */
    def statusTime : Instant

    private[core] def logTaskMessage(logger: Logger)

    def compare(that: TaskInfoLike): Int = {
      (this.id, that.id) match {
        case (Some(_), None)    => -1
        case (None, Some(_))    => 1
        case (None, None)       => this.status.ordinal - that.status.ordinal
        case (Some(l), Some(r)) => (l - r).toInt
      }
    }
  }

  object TimePoint {
    def parse(s: String, f: Int => TaskStatus): TimePoint = {
      s.split(',').toList match {
        case _ :: ordinal :: instant :: _ =>
          TimePoint(
            status = f(ordinal.toInt),
            instant = Instant.parse(instant)
          )
        case _ =>
          throw new IllegalArgumentException(s"Could not parse TimePoint '$s'")
      }
    }
  }

  /** A tuple representing the instant the task was set to the given status. */
  case class TimePoint(status: TaskStatus, instant: Instant) {
    override def toString: String = {
      s"${this.status.name},${this.status.ordinal},${this.instant.toString},${this.status.description}"
    }
  }

  /** Execution information associated with a task.  Any execution system should extend this class to store
    * their specific metadata.
    * @param task the task in question
    * @param initStatus the initial status
    * @param id the unique id, if any, of the task.  The id may not be set until execution in some execution systems.
    * @param attempts the number of execution attempts (one-based)
    * @param script the path to the execution script for the task, if any
    * @param log the path to the log file for the task, if any
    * @param resources the resources that the tasks used during execution or was scheduled with
    * @param exitCode the exit-code for the task, if any
    * @param throwable a throwable generated during execution, if any
    */
  private[core] abstract class TaskInfo
  (
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    // Core info
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    var task: Task,
    initStatus: TaskStatus,
    var id: Option[TaskId]               = None,
    var attempts : Int                   = 1,/** How many times it has been attempted. */
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    // Execution-Specific Information
    //
    // These properties are specific to running things in a local (bash/process) environment.  We may
    // have other executors that don't have these, and so for now, they are included, but made options.
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    var script     : Option[FilePath]    = None,
    var log        : Option[FilePath]    = None,
    var resources  : Option[ResourceSet] = None,
    var exitCode   : Option[Int]         = None,
    var throwable  : Option[Throwable]   = None
  ) extends TaskInfoLike {

    if (attempts < 1) throw new RuntimeException("attempts must be greater than zero")

    /** The set of time points that contains time points of the instant a status was set. */
    private val _timePoints: mutable.ArrayBuffer[TimePoint] = new mutable.ArrayBuffer[TimePoint]()

    /** Initialization! */
    {
      // set the time the status was initially set to NOW!
      this._timePoints.append(TimePoint(initStatus, Instant.now))

      // Update the reference in [[Task]] to this.
      task.taskInfo = this
    }

    /** Updates the instant for the status if the given status different from the current status or the current status
      * is not set. */
    private[core] final def update(status: TaskStatus, instant: Instant): Unit = if (status != this.status) {
      this._timePoints.append(TimePoint(status, instant))
    }

    /** Gets the latest instant for the status, if any. */
    private[core] final def apply(status: TaskStatus): Option[Instant] = {
      // find the last status with the given time point
      this._timePoints.reverseIterator.find(_.status == status).map(_.instant)
    }

    /** Gets the latest instant for any status of instance of [[T]]. */
    private[core] final def latestStatus[T: ClassTag]: Option[Instant] = {
      this._timePoints.reverseIterator.find { timePoint =>
        timePoint.status match {
          case _: T => true
          case _    => false
        }
      }.map(_.instant)
    }

    /** Sets the current status of the task, as well as the instant for the status. */
    private[core] def status_=(status: TaskStatus): Instant = {
      val instant = Instant.now
      update(status, instant)
      this.task._executor.foreach(_.record(info=this))
      instant
    }

    /** The current status of the task. */
    def status     : TaskStatus = this.timePoints.last.status

    /** The instant the task reached a given status. */
    final def timePoints :  Traversable[TimePoint] = this._timePoints.toIndexedSeq

    /** The instant the task reached the current status. */
    final def statusTime : Instant = apply(this.status).get // Break it down (Oh-oh-oh-oh-oh-oh-oh-oh-oh oh-oh) (Oh-oh-oh-oh-oh-oh-oh-oh-oh oh-oh). Stop. Status time

    /** Gets the instant that the task was submitted to the execution system. */
    protected[core] def submissionDate: Option[Instant]

    /** The instant the task started executing. */
    protected[core] def startDate: Option[Instant]

    /** The instant that the task finished executing. */
    protected[core] def endDate: Option[Instant]

    /** Gets the execution and total time. */
    protected[core] def executionAndTotalTime: (String, String) = (submissionDate, startDate, endDate) match {
      case (Some(submission), Some(start), Some(end)) =>
        val sinceSubmission = Duration.between(submission, end)
        val sinceStart      = Duration.between(start, end)
        (formatElapsedTime(sinceStart.getSeconds), formatElapsedTime(sinceSubmission.getSeconds))
      case _ => ("NA", "NA")
    }

    /** Logs a message for the given task. */
    private[core] def logTaskMessage(logger: Logger): Unit = {
      val resourceMessage = this.resources match {
        case Some(r) => s" with ${r.cores} cores and ${r.memory} memory"
        case None    => ""
      }
      logger.info(s"'${this.task.name}' : ${this.status} on attempt #${this.attempts}" + resourceMessage)
    }
  }

  /** Helper class for Tarjan's strongly connected components algorithm */
  private class TarjanData {
    var index: Int = 0
    val stack: mutable.Stack[Task] = new mutable.Stack[Task]() // FIXME: mutable.Stack is deprecated in 2.12!
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
    val toVisit: mutable.Set[Task] = new mutable.HashSet[Task]() {
      add(task)
    }
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
        data.lowLink.put(v, math.min(data.lowLink(v), data.lowLink(w)))
      }
      else if (data.onStack(w)) {
        // Successor w is in stack S and hence in the current SCC
        data.lowLink.put(v, math.min(data.lowLink(v), data.lowLink(w)))
      }
    }

    // If v is a root node, pop the stack and generate an SCC
    if (data.indexes(v) == data.lowLink(v)) {
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
 * 1. Any tasks on which it depends are added (see [[Dependable.==>()]]).
 * 2. When all tasks on which it is dependent have completed, the [[dagr.core.tasksystem.Task#getTasks]] method
 *    is called to create a set of tasks. This task becomes dependent on any task that
 *    is returned that is not itself.
 * 3. When all newly dependent tasks from #2 are complete, as well as this task, the
 *    [[Task#onComplete]] method is called to perform any light-weight modification of this
 *    task.
 */
trait Task extends Dependable {
  /** The executor that is responsible for executing this task, None if not set. */
  private[core] var _executor : Option[Executor] = None

  /** The execution information about this task, or None if not being executed. */
  private[core] var _taskInfo : Option[TaskInfo] = None
  private[dagr] def taskInfo  : TaskInfo = this._taskInfo.get
  private[core] def taskInfo_=(info: TaskInfo) = {
    this._taskInfo = Some(info)
    this._executor.foreach(_.record(info))
  }

  /** Get the task info for dagr.core.execsystem */
  private[core] def execsystemTaskInfo : TaskExecutionInfo = this._taskInfo.getOrElse(unreachable(s"Task info should be defined for task '$name'")) match {
    case info: TaskExecutionInfo => info
    case info => throw new IllegalStateException(s"For task info, expected type 'TaskExecutionInfo' but found '${info.getClass.getSimpleName}")
  }

  /** The name of the task. */
  var name: String = getClass.getSimpleName

  /* The set of tasks that this task depends on. */
  private val dependsOnTasks    = new ListBuffer[Task]()
  /* The set of tasks that depend on this task. */
  private val dependedOnByTasks = new ListBuffer[Task]()

  /** Gets the sequence of tasks that this task depends on.. */
  protected[dagr] def tasksDependedOn: Traversable[Task] = this.dependsOnTasks.toList

  /** Gets the sequence of tasks that depend on this task. */
  protected[dagr] def tasksDependingOnThisTask: Traversable[Task] = this.dependedOnByTasks.toList

  /** Must be implemented to handle the addition of a dependent. */
  override def addDependent(dependent: Dependable): Unit = dependent.headTasks.foreach(t => {
      t.dependsOnTasks += this
      this.dependedOnByTasks += t
    })

  /** Removes this as a dependency for other */
  override def !=>(other: Dependable): Unit = other.headTasks.foreach(_.removeDependency(this))

  override def headTasks: Traversable[Task] = Seq(this)
  override def tailTasks: Traversable[Task] = Seq(this)
  override def allTasks: Traversable[Task]  = Seq(this)

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
    * All tasks, multi-tasks, workflows, and other task-like-entities should the [[getTasks]] method.
    *
    * All executors should call this method to get the list of tasks to execute.
    *
    * @return the list of tasks of to run.
    */
  final def make(): Traversable[_ <: Task] = {
    val tasks = getTasks.toSeq
    tasks.foreach(_._executor = this._executor)
    this._executor.foreach(_.register(this, tasks:_*))
    tasks
  }

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
  protected def getTasks: Traversable[_ <: Task]

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
