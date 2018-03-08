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

import com.fulcrumgenomics.commons.CommonsDef.{DirPath, unreachable}
import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.exec.{ExecDef, Executor => RootExecutor, SystemResources}
import dagr.core.execsystem2.TaskStatus._
import dagr.core.tasksystem.Task.{TaskInfo => RootTaskInfo}
import dagr.core.tasksystem.{Retry, Task}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, blocking}
import scala.util.{Failure, Success}

// TODO: support failFast

/** Executes a set of tasks executor ([[TaskExecutor]]) given a (root) task to execute.
  */
trait Executor[T<:Task] extends RootExecutor {
  /** Start the execution of this task and all tasks that depend on it.  Returns the number of tasks that were not
    * executed.  A given task should only be attempted once. */
  def execute(rootTask: Task): Int

  /** Returns true if the task is part of the execution graph, false otherwise.  Since the graph is lazily built,
    * a task may not yet be contained in the graph. */
  def contains(task: Task): Boolean

  /** Returns the resources used by the underlying task executor(s). */
  def resources: Option[SystemResources] = Some(this.taskExecutor.resources)

  /** Returns the task status by ordinal */
  final def statusFrom(ordinal: Int): TaskStatus = TaskStatus.withValue(ordinal)

  /** The list of statuses ordered by ordinal */
  def statuses: Seq[TaskStatus] = TaskStatus.values

  /** True if the task is running, false otherwise. */
  def running(task: Task): Boolean = task.taskInfo.status == Running

  /** True if the task is ready for execution (no dependencies), false otherwise. */
  def queued(task: Task): Boolean = task.taskInfo.status == Queued || task.taskInfo.status == Submitted

  /** True if the task has failed, false otherwise. */
  def failed(task: Task): Boolean = task.taskInfo.status.isInstanceOf[Failed]

  /** True if the task has succeeded, false otherwise. */
  def succeeded(task: Task): Boolean = task.taskInfo.status == SucceededExecution

  /** True if the task has completed regardless of status, false otherwise. */
  def completed(task: Task): Boolean = task.taskInfo.status.isInstanceOf[Completed]

  /** True if the task has unmet dependencies, false otherwise. */
  def pending(task: Task): Boolean = task.taskInfo.status == Pending

  /** Returns the executor that execute tasks. */
  protected def taskExecutor: TaskExecutor[T]
}

object Executor {
  /** Creates a default graph executor given a task executor */
  def apply[T<:Task](taskExecutor: TaskExecutor[T])(implicit ex: ExecutionContext): Executor[T] =
    new ExecutorImpl(taskExecutor=taskExecutor)
}

/** An implementation of an executor of a tasks that have dependencies.
  *
  * This uses [[Future]]s to execute discrete steps of a task's life.  For example, a [[Future]] is returned that completes
  * successfully when the underlying task executes successfully, but returns a [[Failure]] if any failure was encountered.
  * Furthermore, this allows for concurrency for handling the transition of states across multiple tasks.
  *
  * Currently, only a single task executor is supported for all tasks that will eventually get executed (i.e. tasks that do
  * not build other tasks).  In the future, we could have a list of partial functions that map a type of task to its
  * associated task executor.
  *
  * A root task is given to the [[dagr.core.execsystem2.ExecutorImpl._execute()]] method to start execution.  Each
  * new task (including the root task) is registered ([[dagr.core.execsystem2.ExecutorImpl.registerTask()]]).  If the
  * given tasks is to be executed (does not create new tasks), it is executed, otherwise the task will wait for all its
  * children to complete successfully (or a single child to fail).  Any failure will propogate to the parent of the
  * failed task and so on.
  *
  * The executor is also responsible for updating the status of the task throughout the task's life.
  *
  * @param taskExecutor the executor for tasks of type [[T]].
  * @param ex the execution context in which to run execution (but not the task execution themselves).
  * @tparam T the type of task that can be individually executed.
  */
private class ExecutorImpl[T<:Task](protected val taskExecutor: TaskExecutor[T])
                                   (implicit ex: ExecutionContext)
  extends Executor[T] with LazyLogging {

  /** The tasks currently known by the executor. */
  private val _tasks: mutable.Set[Task] = ExecDef.concurrentSet()

  /** A lock to synchronize when the task execution or dependency information is updated. */
  private val lock: Object = _tasks

  /** The tasks currently known by the executor. */
  def tasks: Traversable[Task] = this._tasks

  /** Returns true if the task is known */
  override def contains(task: Task): Boolean = _tasks.contains(task)

  /** Returns the log directory. */
  def logDir: DirPath = this.taskExecutor.logDir

  /** Start the execution of this task and all tasks that depend on it.  Returns the number of tasks that were not
    * executed.  A given task should only be attempted once. */
  protected def _execute(rootTask: Task): Int = {
    // Catch failure if the initial registration fails.
    val rootFuture: Future[Task] = failFutureWithTaskStatus(rootTask) {
      Future {
        lockIt {
          registerTask(rootTask) match {
            case None        => throw new IllegalArgumentException(s"Task '${rootTask.name}' already attempted.")
            case Some(false) => throw new IllegalArgumentException(s"Task '${rootTask.name}' depends on ${rootTask.tasksDependedOn.size} tasks.")
            case Some(true)  => Unit
          }
        }
        rootTask
      }
    } flatMap { task: Task =>
      processTask(task)
    }

    // Wait forever for it to run
    Await.ready(rootFuture, Duration.Inf)
    rootFuture onComplete {
      case Success(t)   => logger.info(s"Completed root task '${t.name}' successfully")
      case Failure(thr) => logger.error(thr, thr.getMessage)
    }

    // Return the number of tasks known not to succeed
    this._tasks.map(_.taskInfo).count { info => !info.status.success }
  }

  /** Process a given task, its sub-tree, and tasks that only depend on this task.
    * 1. track any tasks that depend on this task, if not already tracked.
    * 2. execute this task and it's sub-tree recursively.
    * 3. wait on tasks that depend on this task that were added in #1 to complete.
    * Returns a future that completes if all dependent tasks complete (from #1), failure otherwise.  Note: the future
    * is not the result of the given task's execution, but of the execution of itself, its sub-tree, and any tasks that
    * depended on it.
    */
  private def processTask(task: Task): Future[Task] = {
    // add any tasks that depend on this task to the graph.  NB: add all the dependents at once for expedience
    // (too many futures if done individually)
    Future {
      lockIt {
        task.tasksDependingOnThisTask map { dependent =>
          // NB: this dependent may have been added by another task
          registerTask(dependent) // returns Option[Boolean]
        }
      }
    } flatMap { _ =>
      buildAndExecute(task) // build and execute the task itself, including its sub-tree
    } map { t: Task =>
      // Get any dependents that can be processed now that this task and its sub-tree have complete execution.
      // Process those tasks.
      require(t.tasksDependedOn.isEmpty,
        s"Removing a task '${t.name}' from the dependency graph that has dependencies: "
          + t.tasksDependedOn.map(_.name).mkString(", "))
      // remove this as a dependency for all other tasks that depend on this task
      t.tasksDependingOnThisTask.flatMap { dependent =>
        dependent.synchronized {
          t !=> dependent
          if (dependent.tasksDependedOn.isEmpty) Some(dependent) else None
        }
      }.map(processTask).toSeq // process them!
    } flatMap { dependentFutures: Seq[Future[Task]] =>
      // wait until all tasks that depended on this task are processed
      Future.sequence(dependentFutures) map { _: Seq[Task] => task }
    }
  }

  /** Builds a task and proceeds based on if it create other tasks or itself should be executed.  Completes when
    * itself and any children have executed.  It does not update the dependency graph. */
  private def buildAndExecute(parent: Task): Future[Task] = failFutureWithTaskStatus(parent) { // tag any failures during build or execution
    requireNoDependencies(parent)

    updateMetadata(parent, Queued)

    buildTask(parent) flatMap {
      case x :: Nil if x == parent => // one task and it returned itself, so execute it
        requireNoDependencies(parent)
        // FIXME: make sure the first "case's" task is of type T
        if (this.taskCaches.isEmpty || this.taskCaches.exists(_.execute(parent))) {
          executeWithTaskExecutor(parent)
        }
        else {
          updateMetadata(parent, ManuallySucceeded)
          Future.successful(parent)
        }
      case childTasks              => // a different task, or more than one task, so build those tasks and execute them
        requireNoDependencies(parent)
        executeMultiTask(parent, childTasks)
    }
  }

  /** Build the task and catch any exceptions during the call to [[Task.make()]]. */
  private def buildTask(task: Task): Future[Seq[Task]] = failWithFailedToBuild(task) {
    requireNoDependencies(task)
    val tasks = task.make().toList
    if (tasks.isEmpty) throw new IllegalArgumentException(s"No tasks built from task: '${task.name}'")
    tasks
  }

  /** Executes the task given a set of child tasks that are to be executed instead.  The success of the task depends
    * on the success of its children.  Isn't that true for most of life. */
  private def executeMultiTask(parent: Task, childTasks: Seq[Task]): Future[Task] = {
    // use `recoverWith` so we set the parent status correctly on failure
    failFutureWithFailedExecution(parent) {
      // fail with [[FailedExecution]] with any failure.
      updateMetadata(parent, Running)

      // For each child task, register any dependent tasks (task that depend on the child task).  The parent task can
      // complete once they have all been executed successfully.
      val childFutures: Future[Seq[Task]] = Future {
        // Register them all at once, since locking is expensive, and we may end up creating many, many futures
        lockIt {
          // check that children of the parent haven't already been seen as this is not currently allowed.  A child can only have one parent!
          childTasks.foreach { child =>
            require(!this._tasks.contains(child), s"child '${child.name}' of parent '${parent.name}' already seen")
          }
          val result = childTasks flatMap { child =>
            // NB: if the child has dependencies, it will get executed when the task with last unmet dependency is removed
            // from the dependency graph
            if (registerTask(child).contains(true)) Some(child) else None
          }
          // check for cyclical dependencies since a new sub-tree has been added
          this.exceptIfCyclicalDependency(parent)
          // NB: returns true if the task has no dependencies, false otherwise
          result
        }
      } flatMap { children: Seq[Task] =>
        Future.sequence(children.map(processTask))
      }

      childFutures map { _ => // is of type Seq[Task]
        // Update the status of the parent to succeeded if *all* children succeeded.
        updateMetadata(parent, SucceededExecution)
        parent
      } recoverWith {
        // in the case of a failure with tagged exception, fail with the underlying throwable
        case thr: TaggedException => Future.failed(thr.thr)
      }
    }
  }

  /** Submits and executes a task vai the task executor. */
  private def executeWithTaskExecutor(task: Task): Future[T] = {
    Future { task.asInstanceOf[T] } flatMap { t: T =>
      val subFuture     = submitAndExecuteFuture(t)
      val onComplFuture = onCompleteFuture      (t, execFuture=subFuture.flatMap(identity))
      val complFuture   = completedTaskFuture   (t, onComplFuture=onComplFuture)
      complFuture
    }
  }

  /** Try and submit the task and execute it with the task executor.  Returns a future that completes when the task
    * can start executing, due to any reason (ex. scheduling or assigning resources, locality, etc.).  The inner future
    * completes after the task has successfully executed. Returns a future that is wrapped so
    * that any failure is tagged as [[FailedSubmission]].  */
  private def submitAndExecuteFuture(task: T): Future[Future[T]] = failFutureWithFailedSubmission(task) {
    // Update the task to be submitted ot the task executor
    requireTaskStatus(task, Queued)
    updateMetadata(task, Submitted)

    // NB: convert to type [[T]] here so that the exception is caught during submission
    Future { task.asInstanceOf[T] } flatMap { t: T =>
      // This future completes when the task has started execution.  A delay may occur to scheduling, resourcing, or any
      // other multitude of reasons.
      // TODO: partial functions here

      this.taskExecutor.execute(t, f = updateToRunning(task)).map { execFuture: Future[T] =>
        // Wrap the execution future so that we can update the status in case of failure
        failFutureWithFailedExecution(t)(execFuture)
      }
      // TODO: could have a status for 'Scheduled' (eg. Submitted -> Scheduled -> Running)
    }
  }

  /** Update the task status to [[Running]].
    * */
  private def updateToRunning(task: T): Unit = {
    requireTaskStatus(task, Submitted)
    updateMetadata(task, Running)
  }

  /** Run the task's [[Task.onComplete]] method once it has executed successfully.  Returns a future that is wrapped so
    * that any failure is tagged as [[FailedOnComplete]]. w*/
  private def onCompleteFuture(task: T, execFuture: Future[T]): Future[T] = {
    // Wrap running the onComplete method in a future!
    def onComplete(): Future[T] = failWithFailedOnComplete(task) {
      task.taskInfo.exitCode.foreach { code =>
        if (!task.onComplete(code)) throw new IllegalArgumentException(s"onComplete failed for task: ${task.name}")
      }
      task
    }

    execFuture flatMap { task: T =>
      requireTaskStatus(task, Running)
      onComplete()
    } recoverWith {
      // only run the onComplete method if the it failed when running
      case taggedException: TaggedException if taggedException.status == FailedExecution =>
        // if the onComplete future completes, fail with the original exception, otherwise it should fail with an
        // onComplete failure
        onComplete() flatMap { _ => Future.failed(taggedException) }
    }
  }

  /** If the given future completes, update the status to [[SucceededExecution]], otherwise attempt to retry the task
    * if it failed during execution. */
  private def completedTaskFuture(task: T, onComplFuture: Future[T]): Future[T] = failFutureWithTaskStatus(task) {
      onComplFuture map { t: T =>
        updateMetadata(t, SucceededExecution)
        require(!this.taskExecutor.contains(t), s"Task was still tracked '${t.name}'")
        t
      } recoverWith {
        // retry only if we failed when running
        case taggedException: TaggedException if taggedException.status == FailedExecution =>
          task match {
            case r: Retry =>
              println("Retry task: will retry: " + r.retry(resources=taskExecutor.resources, task.taskInfo))
            case _ => Unit
          }
          task match {
            case r: Retry if r.retry(resources=taskExecutor.resources, task.taskInfo) =>
              // Queue and execute it again
              task.taskInfo.attempts += 1
              updateMetadata(task, Queued)
              this.executeWithTaskExecutor(task)
            case _ =>
              Future.failed(taggedException)
          }
      }
  }

  /** Update the status of of a task and returns the most current copy of the metadata */
  private def updateMetadata(task: Task, status: TaskStatus): RootTaskInfo = lockIt {
    if (!this._tasks.contains(task)) {
      // NB: TaskInfo's constructor will assign a reference from task to info
      new TaskInfo(
        task       = task,
        initStatus = status
      )
      this._tasks.add(task)
    }
    else {
      task.taskInfo.status = status
    }
    task.taskInfo
  }

  /** Registers the task.  If it already has been registered, does nothing.  Adds the task to the dependency graph
    * and sets the status to Pending. Returns None if the task was previously added, true if the task was added and
    * has no dependencies, and false otherwise */
  private def registerTask(task: Task): Option[Boolean] = lockIt {
    if (this._tasks.contains(task)) {
      None
    }
    else {
      updateMetadata(task, Pending)
      Some(task.tasksDependedOn.isEmpty)
    }
  }

  /** Provides synchronization and signals that this may block. */
  private def lockIt[A](body: =>A): A = blocking { this.lock.synchronized(body) }

  /** Set the status to the failed and add the throwable to the failures map for this task */
  private def fail(task: Task, thr: Throwable, status: TaskStatus): Unit = lockIt {
    require(status.isInstanceOf[Failed], s"$status was not of type Failed")
    updateMetadata(task, status)
  }

  /** Ensure that the task has the given status. */
  private def requireTaskStatus(task: Task, status: TaskStatus): Unit = {
    val _status = task.taskInfo.status
    require(_status == status, s"Task '${task.name}' was not in ${status.name} state: ${_status.name}")
  }

  /** Ensure that the task has no dependencies.  If missingOk is set, then if do not throw an exception if the task
    * is not known. */
  private def requireNoDependencies(task: Task): Unit =
    require(task.tasksDependedOn.isEmpty, s"Task ${task.name} has dependencies.")

  /** Returns the [[Future[T]] when failed has its throwable tagged with a status using [[TaggedException]] */
  private def failFutureWithTaskStatus[A](task: Task, status: TaskStatus=FailedUnknown)(future: Future[A]): Future[A] = {
    // If there was any failure, the throwable should be a [[TaggedException]] so that the appropriate failed status can
    // be set.
    future recoverWith {
      case thr: TaggedException =>
        require(thr.status.isInstanceOf[Failed] || Seq(Running, Queued).contains(thr.status),
          s"Expected status to be Failed, Running, or Queued, but found ${thr.status}")
        Future.failed[A](thr)
      case thr: Throwable       =>
        fail(task, thr, status)
        Future.failed[A](TaggedException(thr=thr, status=status))
      case other                => unreachable(s"Expected a throwable, found $other")
    }
  }
  //def failFutureWithFailedToBuild   [A](task: Task)(future: Future[A]): Future[A] = failFutureWithTaskStatus(task, FailedToBuild   )(future)
  def failFutureWithFailedSubmission[A](task: Task)(future: Future[A]): Future[A] = failFutureWithTaskStatus(task, FailedSubmission)(future)
  def failFutureWithFailedExecution [A](task: Task)(future: Future[A]): Future[A] = failFutureWithTaskStatus(task, FailedExecution )(future)
  //def failFutureWithFailedOnComplete[A](task: Task)(future: Future[A]): Future[A] = failFutureWithTaskStatus(task, FailedOnComplete)(future)

  /** Returns a [[Future[T]] with a given body that when failed has its throwable tagged with a status using
    * [[TaggedException]] */
  private def failWithTaskStatus[A](task: Task, status: TaskStatus=FailedUnknown, body: =>A): Future[A] = {
    failFutureWithTaskStatus(task=task, status=status)(future = Future[A] { body })
  }
  def failWithFailedToBuild   [A](task: Task)(body: => A): Future[A] = failWithTaskStatus(task, FailedToBuild,    body)
  //def failWithFailedSubmission[A](task: Task)(body: => A): Future[A] = failWithTaskStatus(task, FailedSubmission, body)
  //def failWithFailedExecution [A](task: Task)(body: => A): Future[A] = failWithTaskStatus(task, FailedExecution,  body)
  def failWithFailedOnComplete[A](task: Task)(body: => A): Future[A] = failWithTaskStatus(task, FailedOnComplete, body)

  /** Throws an exception if there is a cycle in the dependency graph.  The exception may have relevant debug
    * information.
    */
  private def exceptIfCyclicalDependency(task: Task): Unit = this.synchronized {
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

/** A little class to store an exception and associated task status. */
private[execsystem2] case class TaggedException(thr: Throwable, status: TaskStatus) extends Exception(thr)
