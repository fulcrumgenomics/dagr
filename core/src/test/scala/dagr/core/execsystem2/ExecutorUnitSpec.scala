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

import java.nio.file.Files
import java.time.Instant

import com.fulcrumgenomics.commons.CommonsDef.DirPath
import dagr.api.models.util.{Cores, ResourceSet, TimePoint}
import dagr.core.FutureUnitSpec
import dagr.api.models.tasksystem
import dagr.core.execsystem2.TaskStatus._
import dagr.core.execsystem2.local.{LocalTaskExecutor, LocalTaskExecutorDefaults}
import dagr.core.tasksystem.Task.{TaskInfo => RootTaskInfo}
import dagr.core.tasksystem.{Pipeline, ShellCommand, Task, UnitTask}

import scala.concurrent.Future
import scala.concurrent.duration.Duration

/** Various methods to help test [[Executor]].  The various methods use [[UnitTask]] and [[Pipeline]] and were
  * written with a [[LocalTaskExecutor]] in mind. */
private[execsystem2] trait ExecutorUnitSpec extends FutureUnitSpec {

  protected def scriptDirectory: DirPath = {
    val dir = Files.createTempDirectory("executorTest.scripts")
    dir.toFile.deleteOnExit()
    dir
  }

  protected def logDirectory: DirPath = {
    val dir = Files.createTempDirectory("executorTest.scripts")
    dir.toFile.deleteOnExit()
    dir
  }

  protected def taskExecutor = new LocalTaskExecutor(scriptDirectory=scriptDirectory, logDirectory=logDirectory) {
    //override def checkTaskCanBeScheduled(task: UnitTask): Option[Future[Future[UnitTask]]] = None
  }

  protected def defaultExecutor: ExecutorImpl[UnitTask] = {
    new ExecutorImpl(taskExecutor=taskExecutor)
  }

  protected def defaultExecutorSingleCore: ExecutorImpl[UnitTask] = {
    val taskExecutor = new LocalTaskExecutor(scriptDirectory=scriptDirectory, logDirectory=logDirectory,
      systemResources=LocalTaskExecutorDefaults.defaultSystemResources.copy(cores=Cores(1)))
    new ExecutorImpl(taskExecutor=taskExecutor)
  }

  protected def defaultExecutorSubmissionException: ExecutorImpl[UnitTask] = {
    val taskExecutor = new LocalTaskExecutor(scriptDirectory=scriptDirectory, logDirectory=logDirectory) {
      override def execute(task: UnitTask, f: => Unit = () => Unit): Future[Future[UnitTask]] = throw new IllegalArgumentException
    }
    new ExecutorImpl(taskExecutor=taskExecutor)
  }

  protected def defaultExecutorFailedSubmission: ExecutorImpl[UnitTask] = {
    val taskExecutor = new LocalTaskExecutor(scriptDirectory=scriptDirectory, logDirectory=logDirectory) {
      override def execute(task: UnitTask, f: => Unit = () => Unit): Future[Future[UnitTask]] = Future.failed(new IllegalArgumentException)
    }
    new ExecutorImpl(taskExecutor=taskExecutor)
  }

  protected def defaultExecutorFailedExecution: ExecutorImpl[UnitTask] = {
    val taskExecutor = new LocalTaskExecutor(scriptDirectory=scriptDirectory, logDirectory=logDirectory) {
      override def execute(task: UnitTask, f: => Unit = () => Unit): Future[Future[UnitTask]] = Future { Future.failed(new IllegalArgumentException) }
    }
    new ExecutorImpl(taskExecutor=taskExecutor)
  }

  protected def defaultExecutorWithTask(task: Task): ExecutorImpl[UnitTask] = {
    val executor = this.defaultExecutor
    ExecutorImplTest.updateMetadata(executor, task, Pending)
    executor
  }

  /** Creates a task that requires no resources and completes immediately. */
  protected def successfulTask: UnitTask = new PromiseTask(Duration.Zero, ResourceSet.empty) with UnitTask withName "successful-task"

  /** Creates a task that has an infinite amount of resources. */
  protected def infiniteResourcesTask: UnitTask = new PromiseTask(Duration.Zero, ResourceSet.infinite) with UnitTask withName "infinite-resources-task"

  /** Creates a task that requires no resources but never halts. */
  protected def infiniteDurationTask: UnitTask = new PromiseTask(Duration.Inf, ResourceSet.empty) with UnitTask withName "infinite-duration-task"

  /** Creates a task that has an infinite amount of resources and never halts. */
  protected def failureTask: UnitTask = new PromiseTask(Duration.Inf, ResourceSet.infinite) with UnitTask withName "failure-task"

  /** A simple pipeline with 1 ==> (2 :: 3) ==> 4. */
  protected def pipeline: Pipeline = new Pipeline() {
    withName("pipeline")
    def task(i: Int): UnitTask = successfulTask withName s"$name-$i"
    override def build(): Unit = {
      root ==> task(0) ==> (task(1) :: task(2)) ==> task(3)
    }
  }

  /** A pipeline with one task. */
  protected def pipelineOneTask: Pipeline = new Pipeline() {
    withName("pipeline")
    override def build(): Unit = {
      root ==> (successfulTask withName s"${this.name}-task")
    }
  }

  /** A pipeline that fails: OK ==> (FAIL :: OK) ==> OK. */
  protected trait PipelineFailureTrait extends Pipeline {
    def failTask: UnitTask
    def okTasks: Seq[UnitTask]
  }
  protected def pipelineFailure: PipelineFailureTrait = new PipelineFailureTrait() {
    withName("pipeline-failure")
    def task(i: Int): UnitTask = successfulTask withName s"ok-task-$i"
    val failTask: UnitTask = new ShellCommand("exit", "1") withName "fail-task"
    val okTasks: Seq[UnitTask] = Seq(task(0), task(1), task(2))
    override def build(): Unit = {
      root ==> okTasks.head ==> (failTask :: okTasks(1)) ==> okTasks.last
    }
  }

  protected def checkStatus(executor: Executor[UnitTask], task: Task, status: TaskStatus): RootTaskInfo = {
    executor.contains(task) shouldBe true
    val info = task.taskInfo
    info.status shouldBe status
    info
  }

  protected def checkSomeStatus(executor: Executor[UnitTask], task: Task, statuses: Set[tasksystem.TaskStatus]): RootTaskInfo = {
    executor.contains(task) shouldBe true
    val info = task.taskInfo
    statuses.contains(info.status) shouldBe true
    info
  }

  protected def checkInfo(executor: Executor[UnitTask], task: Task, statuses: Seq[TaskStatus], attempts: Int = 1): Unit = {
    val info = checkStatus(executor, task, statuses.last)
    checkTimePoints(timePoints=info.timePoints, statuses=statuses)
    info.attempts shouldBe attempts
  }

  protected def checkPipelineFailure(pipeline: PipelineFailureTrait, executor: Executor[UnitTask]): Unit = {

    // root
    {
      checkInfo(executor=executor, task=pipeline, statuses=Seq(Pending, Queued, Running, FailedExecution))
    }

    // task1
    {
      val task = pipeline.okTasks.head
      checkInfo(executor=executor, task=task, statuses=Seq(Pending, Queued, Submitted, Running, SucceededExecution))
    }

    // task2
    {
      val task = pipeline.okTasks(1)
      checkInfo(executor=executor, task=task, statuses=Seq(Pending, Queued, Submitted, Running, SucceededExecution))
    }

    // task3
    {
      val task = pipeline.okTasks.last
      checkInfo(executor=executor, task=task, statuses=Seq(Pending))
    }

    // failTask
    {
      val task = pipeline.failTask
      checkInfo(executor=executor, task=task, statuses=Seq(Pending, Queued, Submitted, Running, FailedExecution))
    }
  }

  /** Checks that the instants are in increasing order. */
  protected def checkInstants(instants: Traversable[Instant]): Unit = {
    instants.toSeq should contain theSameElementsInOrderAs instants.toSeq.sorted
  }

  /** Checks that the time points contains the same elements in order for the given statuses, and that the instants
    * are in increasing order of time ordered by the input list statuses .*/
  protected def checkTimePoints(timePoints: Traversable[TimePoint], statuses: Seq[TaskStatus]): Unit = {
    timePoints.map(_.status) should contain theSameElementsAs statuses
    checkInstants(timePoints.map(_.instant))
  }
}
