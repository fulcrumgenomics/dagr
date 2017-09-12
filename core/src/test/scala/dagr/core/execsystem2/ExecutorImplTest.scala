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

import dagr.api.models.util.ResourceSet
import dagr.core.execsystem2.TaskStatus._
import dagr.core.execsystem2.local.LocalTaskExecutor
import dagr.core.tasksystem.Task.{TaskInfo => RootTaskInfo}
import dagr.core.tasksystem._
import org.scalatest.PrivateMethodTester

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

/** A place to store methods that call private methods in [[ExecutorImpl]]. */
private object ExecutorImplTest extends PrivateMethodTester {

  private val processTaskDecorate = PrivateMethod[Future[Task]]('processTask)
  def processTask(executor: ExecutorImpl[UnitTask], task: Task): Future[Task] = {
    executor invokePrivate processTaskDecorate(task)
  }

  private val buildAndExecuteDecorate = PrivateMethod[Future[Task]]('buildAndExecute)
  def buildAndExecute(executor: ExecutorImpl[UnitTask], task: Task): Future[Task] = {
    executor invokePrivate buildAndExecuteDecorate(task)
  }

  private val buildTaskDecorate = PrivateMethod[Future[Seq[Task]]]('buildTask)
  def buildTask(executor: ExecutorImpl[UnitTask], task: Task): Future[Seq[Task]] = {
    executor invokePrivate buildTaskDecorate(task)
  }

  private val executeWithTaskExecutorDecorate = PrivateMethod[Future[UnitTask]]('executeWithTaskExecutor)
  def executeWithTaskExecutor(executor: ExecutorImpl[UnitTask], task: Task): Future[UnitTask] = {
    executor invokePrivate executeWithTaskExecutorDecorate(task)
  }

  private val executeMultiTaskDecorate = PrivateMethod[Future[Task]]('executeMultiTask)
  def executeMultiTask(executor: ExecutorImpl[UnitTask], task: Task, childTasks: Seq[Task]): Future[Task] = {
    executor invokePrivate executeMultiTaskDecorate(task, childTasks)
  }

  private val submitAndExecuteFutureDecorate = PrivateMethod[Future[Future[UnitTask]]]('submitAndExecuteFuture)
  def submitAndExecuteFuture(executor: ExecutorImpl[UnitTask], task: Task): Future[Future[UnitTask]] = {
    executor invokePrivate submitAndExecuteFutureDecorate(task)
  }

  private val onCompleteFutureDecorate = PrivateMethod[Future[UnitTask]]('onCompleteFuture)
  def onCompleteFuture(executor: ExecutorImpl[UnitTask], task: Task, execFuture: Future[UnitTask]): Future[UnitTask] = {
    executor invokePrivate onCompleteFutureDecorate(task, execFuture)
  }

  private val completedTaskFutureDecorate = PrivateMethod[Future[UnitTask]]('completedTaskFuture)
  def completedTaskFuture(executor: ExecutorImpl[UnitTask], task: Task, onComplFuture: Future[UnitTask]): Future[UnitTask] = {
    executor invokePrivate completedTaskFutureDecorate(task, onComplFuture)
  }

  private val requireNoDependenciesDecorate = PrivateMethod[Unit]('requireNoDependencies)
  def requireNoDependencies(executor: ExecutorImpl[UnitTask], task: Task): Unit = {
    executor invokePrivate requireNoDependenciesDecorate(task)
  }

  private val updateMetadataDecorate = PrivateMethod[RootTaskInfo]('updateMetadata)
  def updateMetadata(executor: ExecutorImpl[UnitTask], task: Task, status: TaskStatus): RootTaskInfo = {
    executor invokePrivate updateMetadataDecorate(task, status)
  }
}

class ExecutorImplTest extends ExecutorUnitSpec with PrivateMethodTester {
  import ExecutorImplTest._

  private def checkTaggedException[T<:Throwable](thr: Throwable, status: TaskStatus)(implicit evidence: ClassTag[T]) : Unit = {
    thr shouldBe a[TaggedException]
    val taggedException = thr.asInstanceOf[TaggedException]
    taggedException.status shouldBe status
    taggedException.thr shouldBe a[T]
  }

  "ExecutorImpl.tasks" should "return the list of currently tracked tasks" in {
    val executor = this.defaultExecutor
    val task = successfulTask

    executor.contains(task) shouldBe false

    whenReady(Future(executor.execute(task))) { t =>
      t shouldBe 0
      executor.tasks.toSeq should contain theSameElementsInOrderAs Seq(task)
      checkStatus(executor, task, SucceededExecution)
    }
  }

  "ExecutorImpl.info" should "return metadata about known tasks" in {
    val executor = this.defaultExecutor
    val task = successfulTask

    executor.contains(task) shouldBe false

    whenReady(Future(executor.execute(task))) { t =>
      t shouldBe 0
      checkStatus(executor, task, SucceededExecution)
    }
  }

  "ExecutorImpl.status" should "return the status about known tasks" in {
    val executor = this.defaultExecutor
    val task = successfulTask

    executor.contains(task) shouldBe false

    whenReady(Future(executor.execute(task))) { t =>
      t shouldBe 0
      executor.contains(task) shouldBe true
      checkStatus(executor, task, SucceededExecution)
    }
  }

  "ExecutorImpl.execute" should "run a single task end-to-end with success" in {
    val executor = this.defaultExecutor
    val root = successfulTask
    executor.execute(root) shouldBe 0
    checkStatus(executor, root, SucceededExecution)
  }

  it should "run a single task end-to-end with failure" in {
    val executor = this.defaultExecutor
    val root = new ShellCommand("exit", "1") withName "exit 1"
    executor.execute(root) shouldBe 1
    checkStatus(executor, root, FailedExecution)
  }

  it should "run a pipeline end-to-end with success" in {
    val executor = this.defaultExecutor
    val pipeline = this.pipeline
    executor.execute(pipeline) shouldBe 0
    checkStatus(executor, pipeline, SucceededExecution)
  }

  it should "run a pipeline end-to-end with failure" in {
    val executor = this.defaultExecutor
    val pipeline = this.pipelineFailure
    executor.execute(pipeline) shouldBe 3 // pipeline and two tasks
    checkStatus(executor, pipeline, FailedExecution)
  }

  it should "fails when the task executor does not support the task" in {
    val executor = this.defaultExecutor
    val root = new Task {
      final def getTasks: Traversable[_ <: this.type] = List(this)
    }
    executor.execute(root) shouldBe 1
    checkStatus(executor, root, FailedSubmission)
  }

  it should "fails when a task cannot be scheduled" in {
    val taskExecutor = new LocalTaskExecutor(scriptDirectory=scriptDirectory, logDirectory=logDirectory)
    val executor = new ExecutorImpl(taskExecutor=taskExecutor)
    val root = infiniteResourcesTask

    val execute = Future { executor.execute(root) }
    whenReady(execute) { result =>
      result shouldBe 1
      checkStatus(executor, root, FailedSubmission)
    }
  }

  it should "should not execute the same task twice" in {
    val root = successfulTask withName "root"
    val executor = this.defaultExecutor

    whenReady(Future { executor.execute(root) }) { res1 =>
      res1 shouldBe 0
      checkStatus(executor, root, SucceededExecution)

      whenReady(Future { executor.execute(root) }) { res2 =>
        res2 shouldBe 1
        checkStatus(executor, root, FailedUnknown)
      }
    }
  }

  "ExecutorImpl.processTask" should "execute with no dependent tasks" in {
    val root = successfulTask withName "root"
    val executor = defaultExecutorWithTask(root)

    whenReady(processTask(executor, root)) { _ =>
      checkStatus(executor, root, SucceededExecution)
    }
  }

  it should "execute a task that depends on this task" in {
    val root = successfulTask withName "root"
    val child = successfulTask withName "child"
    root ==> child
    val executor = defaultExecutorWithTask(root)

    executor.contains(child) shouldBe false

    whenReady(processTask(executor, root)) { _ =>
      checkStatus(executor, root, SucceededExecution)
      checkStatus(executor, child, SucceededExecution)
    }
  }

  it should "execute a task that depends on tasks already seen" in {
    // NB: I have no way to verify that it was actually ** not ** added
    val root = successfulTask withName "root"
    val child = successfulTask withName "child"
    root ==> child
    val executor = defaultExecutorWithTask(root)

    updateMetadata(executor, child, Pending)

    executor.contains(child) shouldBe true

    whenReady(processTask(executor, root)) { _ =>
      checkStatus(executor, root, SucceededExecution)
      checkStatus(executor, child, SucceededExecution)
    }
  }

  it should "execute and recurse on children" in {
    val root = successfulTask withName "root"
    val middle = successfulTask withName "middle"
    val leaf = successfulTask withName "leaf"
    root ==> middle ==> leaf
    val executor = defaultExecutorWithTask(root)

    updateMetadata(executor, root, Pending)

    executor.contains(middle) shouldBe false
    executor.contains(leaf) shouldBe false

    whenReady(processTask(executor, root)) { _ =>
      executor.contains(middle) shouldBe true
      executor.contains(leaf) shouldBe true

      checkStatus(executor, root, SucceededExecution)
      checkStatus(executor, middle, SucceededExecution)
      checkStatus(executor, leaf, SucceededExecution)
    }
  }

  it should "execute and recurse on children when the middle child fails" in {
    val root = successfulTask withName "root"
    val middle = failureTask withName "middle-fail"
    val leaf = failureTask withName "leaf" // use a failureTask to prove it never gets run!
    root ==> middle ==> leaf
    val executor = defaultExecutorWithTask(root)

    updateMetadata(executor, root, Pending)
    executor.contains(middle) shouldBe false
    executor.contains(leaf) shouldBe false

    whenReady(processTask(executor, root).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
      executor.contains(middle) shouldBe true
      executor.contains(leaf) shouldBe true

      checkStatus(executor, root, SucceededExecution)
      checkStatus(executor, middle, FailedSubmission)
      checkStatus(executor, leaf, Pending)
    }
  }

  it should "execute and recurse on children when the leaf child fails" in {
    val root = successfulTask withName "root"
    val middle = successfulTask withName "middle"
    val leaf = failureTask withName "leaf-fail"
    root ==> middle ==> leaf
    val executor = defaultExecutorWithTask(root)
    
    updateMetadata(executor, root, Pending)
    executor.contains(middle) shouldBe false
    executor.contains(leaf) shouldBe false

    whenReady(processTask(executor, root).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
      executor.contains(middle) shouldBe true
      executor.contains(leaf) shouldBe true

      checkStatus(executor, root, SucceededExecution)
      checkStatus(executor, middle, SucceededExecution)
      checkStatus(executor, leaf, FailedSubmission)
    }
  }

  it should "complete when there was a dependent task with an unmet dependency" in {
    val root = successfulTask withName "root"
    val child = successfulTask withName "middle"
    val otherRoot = successfulTask withName "middle"
    root ==> child
    otherRoot ==> child
    val executor = defaultExecutorWithTask(root)

    updateMetadata(executor, root, Pending)
    updateMetadata(executor, otherRoot, Pending)
    executor.contains(child) shouldBe false

    // run root, but child depends on other-root
    whenReady(processTask(executor, root)) { _ =>
      executor.contains(root) shouldBe true
      executor.contains(child) shouldBe true
      executor.contains(otherRoot) shouldBe true

      checkStatus(executor, root, SucceededExecution)
      checkStatus(executor, child, Pending)
      checkStatus(executor, otherRoot, Pending)
    }

    // run other-root to release child
    whenReady(processTask(executor, otherRoot)) { _ =>
      executor.contains(root) shouldBe true
      executor.contains(child) shouldBe true
      executor.contains(otherRoot) shouldBe true

      checkStatus(executor, root, SucceededExecution)
      checkStatus(executor, child, SucceededExecution)
      checkStatus(executor, otherRoot, SucceededExecution)
    }
  }

  "ExecutorImpl.buildAndExecute" should "build and execute a unit task successfully" in {
    val task = successfulTask
    val executor = defaultExecutorWithTask(task)

    whenReady(buildAndExecute(executor, task)) { t =>
      checkStatus(executor, t, SucceededExecution)
    }
  }

  it should "build and execute a pipeline successfully" in {
    val pipeline = this.pipeline
    val executor = defaultExecutorWithTask(pipeline)

    whenReady(buildAndExecute(executor, pipeline)) { t =>
      checkStatus(executor, t, SucceededExecution)
    }
  }

  it should "build and execute a pipeline to failure" in {
    val pipeline = this.pipelineFailure
    val executor = defaultExecutorWithTask(pipeline)

    whenReady(buildAndExecute(executor, pipeline).failed) { thr: Throwable =>
      checkTaggedException[IllegalStateException](thr, FailedExecution)
    }
  }

  it should "throw an exception if the task has dependencies" in {
    val executor = this.defaultExecutor
    val root = failureTask

    val child = successfulTask
    root ==> child
    updateMetadata(executor, root, Pending).status shouldBe Pending
    updateMetadata(executor, child, Pending).status shouldBe Pending
    an[IllegalArgumentException] should be thrownBy {
      Await.result(buildAndExecute(executor, child), Duration("1s"))
    }
  }

  "ExecutorImpl.buildTask" should "succeed if one or more tasks where built" in {
    val task = failureTask
    val executor = defaultExecutorWithTask(task)

    whenReady(buildTask(executor, task)) { tasks =>
      tasks should contain theSameElementsInOrderAs Seq(task)
    }
  }

  it should "fail if no tasks were built" in {
    val executor = this.defaultExecutor
    val task = new Task {
      override def getTasks = Nil
    }

    whenReady(buildTask(executor, task).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedToBuild)
    }
  }

  "ExecutorImpl.executeWithTaskExecutor" should "execute a task successfully end-to-end" in {
    val task = successfulTask
    val executor = defaultExecutorWithTask(task)

    // scheduled and run immediately, onComplete throws
    updateMetadata(executor, task, Queued).status shouldBe Queued

    whenReady(executeWithTaskExecutor(executor, task)) { t: UnitTask =>
      checkStatus(executor, t, SucceededExecution)
    }
  }

  it should "fail when any exception was thrown (not in a future)" in {
    val executor = this.defaultExecutorSubmissionException
    // scheduled and run immediately
    val task = successfulTask
    updateMetadata(executor, task, Queued).status shouldBe Queued

    whenReady(executeWithTaskExecutor(executor, task).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
    }
  }

  it should "fail when any future fails" in {
    {
      val executor = this.defaultExecutorFailedSubmission
      // scheduled and run immediately
      val task = successfulTask
      updateMetadata(executor, task, Queued).status shouldBe Queued

      whenReady(executeWithTaskExecutor(executor, task).failed) { thr =>
        checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
      }
    }

    {
      val executor = this.defaultExecutorFailedExecution
      // scheduled and run immediately, onComplete throws
      val task = successfulTask
      updateMetadata(executor, task, Queued).status shouldBe Queued

      whenReady(executeWithTaskExecutor(executor, task).failed) { thr =>
        checkTaggedException[IllegalArgumentException](thr, FailedExecution)
      }
    }
  }

  "ExecutorImpl.executeMultiTask" should "execute a pipeline successfully" in {
    val parent = this.pipeline
    val pipeline = this.pipeline
    val children = pipeline.getTasks.toSeq

    // break the link with root
    children.foreach { child =>
      pipeline.root !=> child
    }

    val executor = defaultExecutorWithTask(parent)

    whenReady(executeMultiTask(executor, parent, children)) { t =>
      t shouldBe parent
      val tasks = parent +: children
      tasks.foreach { task =>
        checkStatus(executor, task, SucceededExecution)
      }
      val parentInstant = parent.taskInfo.get(SucceededExecution).value
      children.foreach { child =>
        val childInstant = child.taskInfo.get(SucceededExecution).value
        checkInstants(Seq(childInstant, parentInstant))
      }
    }
  }

  it should "execute successfully when the parent is a [[UnitTask]]" in {
    val executor = this.defaultExecutor
    val parent = successfulTask
    val pipeline = this.pipeline
    val children = pipeline.getTasks.toSeq

    // break the link with root
    children.foreach { child =>
      pipeline.root !=> child
    }

    whenReady(executeMultiTask(executor, parent, children)) { t =>
      t shouldBe parent
      val tasks = parent +: children
      tasks.foreach { task =>
        checkStatus(executor, task, SucceededExecution)
      }
      val parentInstant = parent.taskInfo.get(SucceededExecution).value
      children.foreach { child =>
        val childInstant = child.taskInfo.get(SucceededExecution).value
        checkInstants(Seq(childInstant, parentInstant))
      }
    }
  }

  it should "execute a pipeline of pipelines" in {
    val executor = this.defaultExecutor
    val pipeline2 = this.pipeline
    val pipeline1 = new Pipeline() {
      override def build(): Unit = root ==> pipeline2
    }
    val parent = pipeline1
    val children = parent.getTasks.toSeq

    whenReady(executeMultiTask(executor, parent, children)) { t =>
      t shouldBe pipeline1
      val tasks = Seq(parent) ++ children ++ pipeline2.tasksDependingOnThisTask
      tasks.foreach { task =>
        checkStatus(executor, task, SucceededExecution)
      }
      val parentInstant = parent.taskInfo.get(SucceededExecution).value
      children.foreach { child =>
        val childInstant = child.taskInfo.get(SucceededExecution).value
        checkInstants(Seq(childInstant, parentInstant))
      }
    }
  }

  it should "fail if any child fails" in {
    val executor = this.defaultExecutor
    val pipeline2 = this.pipelineFailure withName "p2"
    val pipeline1 = new Pipeline() {
      name = "p1"
      override def build(): Unit = root ==> pipeline2
    }
    val parent = pipeline1
    val children = parent.getTasks.toSeq

    whenReady(executeMultiTask(executor, parent, children).failed) { thr: Throwable =>
      checkTaggedException[IllegalStateException](thr, FailedExecution)

      val okTasks = Seq(pipeline2.okTasks.head)
      okTasks.foreach { task =>
        checkStatus(executor, task, SucceededExecution)
      }

      val failTasks = Seq(pipeline2.failTask, pipeline2, pipeline1)
      val failInstants = failTasks.map { task =>
        checkStatus(executor, task, FailedExecution)
        task.taskInfo.get(FailedExecution).value
      }
      checkInstants(failInstants)

      checkStatus(executor, pipeline2.okTasks.last, Pending)

      // NB: pipeline2.okTasks(1) may have completed
      Seq(SucceededExecution, Running, Submitted, Queued) should contain (pipeline2.okTasks(1).taskInfo.status)
    }
  }

  it should "fail if any child is already part of the dependency graph" in {
    val root = successfulTask withName "root"
    val child = successfulTask withName "child"

    root ==> child

    val executor = defaultExecutorWithTask(root)
    updateMetadata(executor, root, Running)

    // naughty
    updateMetadata(executor, child, Pending)

    whenReady(executeMultiTask(executor, root, Seq(child)).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedExecution)

      checkStatus(executor, child, Pending)
      checkStatus(executor, root, FailedExecution)
    }
  }

  "ExecutorImpl.submissionFuture" should "throw an exception if the task status was not Queued" in {
    val executor = this.defaultExecutor
    val task = infiniteResourcesTask

    an[NoSuchElementException] should be thrownBy submitAndExecuteFuture(executor, task)

    updateMetadata(executor, task, Pending).status shouldBe Pending
    an[IllegalArgumentException] should be thrownBy submitAndExecuteFuture(executor, task)
  }

  it should "fail if TaskExecutor.execute throws an exception" in {
    // A task executor that throws an exception
    val executor = defaultExecutorSubmissionException
    val task = infiniteDurationTask
    updateMetadata(executor, task, Queued).status shouldBe Queued

    whenReady(submitAndExecuteFuture(executor, task).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
    }
  }


  it should "fail if the task could not be submitted" in {
    // A task executor that fails the outer future execute method
    val executor = defaultExecutorFailedSubmission
    val task = infiniteDurationTask
    updateMetadata(executor, task, Queued).status shouldBe Queued

    whenReady(submitAndExecuteFuture(executor, task).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
    }
  }

  it should "fails when a task cannot be scheduled" in {
    val taskExecutor = new LocalTaskExecutor(scriptDirectory=scriptDirectory, logDirectory=logDirectory)
    val executor = new ExecutorImpl(taskExecutor=taskExecutor)
    val root = infiniteResourcesTask

    updateMetadata(executor, root, Queued).status shouldBe Queued

    whenReady(submitAndExecuteFuture(executor, root).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
    }
  }

  it should "have status FailedExecution when the task fails execution" in {
    val executor = defaultExecutorFailedExecution
    val task = infiniteDurationTask
    updateMetadata(executor, task, Queued).status shouldBe Queued

    whenReady(submitAndExecuteFuture(executor, task)) { execFuture =>
      checkSomeStatus(executor, task, Set(Submitted, FailedExecution))
      whenReady(execFuture.failed) { thr =>
        checkStatus(executor, task, FailedExecution)
      }
    }
  }

  it should "have status FailedSubmission when the task cannot be scheduled" in {
    val executor = this.defaultExecutor
    val task = infiniteResourcesTask // NB: infinite resources
    updateMetadata(executor, task, Queued).status shouldBe Queued

    whenReady(submitAndExecuteFuture(executor, task).failed) { thr =>
      checkTaggedException[IllegalArgumentException](thr=thr, status=FailedSubmission)
      checkStatus(executor, task, FailedSubmission)
    }
  }

  it should "have status Queued when the task cannot be scheduled (but not yet running)" in {
    val executor = this.defaultExecutorSingleCore
    val blockingTask  = new PromiseTask(Duration.Inf, ResourceSet(1,0)) // NB: one core, infinite duration
    val waitingTask   = new PromiseTask(Duration.Inf, ResourceSet(1,0)) // NB: one core, infinite duration

    updateMetadata(executor, blockingTask, Queued).status shouldBe Queued
    updateMetadata(executor, waitingTask, Queued).status shouldBe Queued

    whenReady(submitAndExecuteFuture(executor, blockingTask)) { blockingExecFuture =>
      blockingExecFuture.isCompleted shouldBe false
      checkStatus(executor, blockingTask, Running)
      checkStatus(executor, waitingTask, Queued)
    }
  }

  "ExecutorImpl.submissionFuture.flatten" should "return a failure if the submission future failed" in {
    // A task executor that fails the outer future execute method
    val executor = defaultExecutorFailedSubmission
    val task = infiniteResourcesTask
    updateMetadata(executor, task, Queued).status shouldBe Queued

    val subFuture  = submitAndExecuteFuture(executor, task)
    val execFuture = subFuture.flatten

    whenReady(execFuture.failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
    }
  }

  it should "return a failure if the execution future failed" in {
    // A task executor that fails the inner future execute method
    val executor = defaultExecutorFailedExecution
    val task = infiniteDurationTask
    updateMetadata(executor, task, Queued).status shouldBe Queued

    val subFuture  = submitAndExecuteFuture(executor, task)
    val execFuture = subFuture.flatten

    whenReady(execFuture.failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedExecution)
    }
  }

  it should "have status Running while running" in {
    val executor = this.defaultExecutor
    val task = infiniteDurationTask
    updateMetadata(executor, task, Queued).status shouldBe Queued

    val subFuture  = submitAndExecuteFuture(executor, task)
    val execFuture = subFuture.flatten

    // wait until it is scheduled
    whenReady(subFuture) { _ => }

    checkStatus(executor, task, Running)
    execFuture.isCompleted shouldBe false
  }


  it should "have status Running after executing" in {
    val executor = this.defaultExecutor
    val task = successfulTask // scheduled and run immediately
    updateMetadata(executor, task, Queued).status shouldBe Queued

    val subFuture = submitAndExecuteFuture(executor, task)

    // wait until it is scheduled
    whenReady(subFuture) { _ =>
      checkSomeStatus(executor, task, Set(Submitted, Running))
      val execFuture = subFuture.flatten
      whenReady(execFuture) { t =>
        checkStatus(executor, t, Running)
      }
    }
  }

  "ExecutorImpl.onCompleteFuture" should "throw an exception if the status was not running" in {
    val executor = this.defaultExecutor
    val task = successfulTask // scheduled and run immediately
    updateMetadata(executor, task, Queued).status shouldBe Queued

    val subFuture  = submitAndExecuteFuture(executor, task)
    val execFuture = subFuture.flatten

    whenReady(execFuture) { _ =>
      updateMetadata(executor, task, Queued).status shouldBe Queued
    }

    an[IllegalArgumentException] should be thrownBy {
      Await.result(onCompleteFuture(executor, task, execFuture), Duration("1s"))
    }
  }

  it should "fail when the onComplete method returns false" in {
    val executor = this.defaultExecutor
    // scheduled and run immediately, onComplete throws
    val task = new PromiseTask(Duration.Zero, ResourceSet.empty) with UnitTask {
      override def onComplete(exitCode: Int): Boolean = false
    }

    updateMetadata(executor, task, Queued).status shouldBe Queued

    val subFuture = submitAndExecuteFuture(executor, task)
    val execFuture = subFuture.flatten
    val onComplFuture = onCompleteFuture(executor, task, execFuture=execFuture)

    whenReady(onComplFuture.failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedOnComplete)
    }
  }

  it should "fail when the execFuture has failed" in {
    val executor = this.defaultExecutor
    // scheduled and run immediately, onComplete throws
    val task = new PromiseTask(Duration.Zero, ResourceSet.empty) with UnitTask {
      override def onComplete(exitCode: Int): Boolean = false
    }

    updateMetadata(executor, task, Running).status shouldBe Running

    val execFuture: Future[UnitTask] = Future.failed(TaggedException(thr=new IllegalArgumentException, status=FailedExecution))
    val onComplFuture = onCompleteFuture(executor, task, execFuture=execFuture)

    whenReady(onComplFuture.failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedExecution)
    }
  }

  it should "succeed when the onComplete method returns true" in {
    val executor = this.defaultExecutor
    // scheduled and run immediately, onComplete throws
    val task = successfulTask
    updateMetadata(executor, task, Queued).status shouldBe Queued

    val subFuture = submitAndExecuteFuture(executor, task)
    val execFuture = subFuture.flatten
    val onComplFuture = onCompleteFuture(executor, task, execFuture=execFuture)

    whenReady(onComplFuture) { t =>
      checkStatus(executor, t, Running)
    }
  }

  "ExecutorImpl.completedTaskFuture" should "fail unknown if task could not be removed from the task executor" in {
    val executor = {
      // an executor that adds the task info back in before returning the future from execute()
      val taskExecutor = new LocalTaskExecutor(scriptDirectory=scriptDirectory, logDirectory=logDirectory) {
        override def execute(task: UnitTask, f: => Unit = () => Unit): Future[Future[UnitTask]] = {
          val future = super.execute(task)
          val info = this.taskInfo(task)
          // add the task info back in before completing!
          future map { execFuture =>
             execFuture map { t => this.taskInfo(task) = info; t }
           }
        }
        override def throwableIfCanNeverBeScheduled(task: UnitTask): Option[Throwable] = None
      }
      new ExecutorImpl(taskExecutor=taskExecutor)
    }
    // scheduled and runs immediately
    val task = successfulTask
    updateMetadata(executor, task, Queued).status shouldBe Queued
    val subFuture = submitAndExecuteFuture(executor, task)
    val execFuture = subFuture.flatten
    val onComplFuture = onCompleteFuture(executor, task, execFuture=execFuture)

    whenReady(completedTaskFuture(executor, task, onComplFuture=onComplFuture).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedUnknown)
    }
  }

  it should "throw an exception if the throwable is not either Failed or Running" in {
    val executor = this.defaultExecutor
    // scheduled immediately, but runs forever
    val task = infiniteDurationTask
    val onComplFuture = Future.failed(TaggedException(thr=new IllegalArgumentException, status=Pending))
    an[IllegalArgumentException] should be thrownBy {
      Await.result(completedTaskFuture(executor, task, onComplFuture=onComplFuture), Duration("1s"))
    }
  }

  it should "update to the failed status when the the future failed" in {
    val executor = this.defaultExecutor
    val task = successfulTask
    val onComplFuture = Future.failed(TaggedException(thr=new IllegalArgumentException, status=FailedOnComplete))
    whenReady(completedTaskFuture(executor, task, onComplFuture=onComplFuture).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedOnComplete)
    }
  }

  it should "fail with the throwable if it is not a TaggedException, and update the status to FailedUnknown" in {
    val executor = this.defaultExecutor
    val task = successfulTask
    val onComplFuture = Future.failed(new IllegalArgumentException)
    updateMetadata(executor, task, Queued).status shouldBe Queued
    whenReady(completedTaskFuture(executor, task, onComplFuture=onComplFuture).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedUnknown)
    }
  }

  it should "complete successfully and update the status to SucceededExecution" in {
    val executor = this.defaultExecutor
    val task = successfulTask
    val onComplFuture = Future.successful(task)
    updateMetadata(executor, task, Queued).status shouldBe Queued
    whenReady(completedTaskFuture(executor, task, onComplFuture=onComplFuture)) { t =>
      checkStatus(executor, task, SucceededExecution)
    }
  }

   it should "should not throw an exception if the task is not tracked and missingOk is true" in {
    val executor = this.defaultExecutor
    val task = successfulTask
    requireNoDependencies(executor, task)
  }

  it should "should throw an exception only if the task has dependencies" in {
    val root  = successfulTask
    val child = successfulTask
    val executor = this.defaultExecutor

    root ==> child
    requireNoDependencies(executor, root)
    an[IllegalArgumentException] should be thrownBy requireNoDependencies(executor, child)
  }

  // TODO: test that onComplete runs when it fails or succeeds

}
