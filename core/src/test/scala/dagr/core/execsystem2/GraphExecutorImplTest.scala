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

import dagr.core.exec.ResourceSet
import dagr.core.execsystem2.TaskStatus._
import dagr.core.execsystem2.local.LocalTaskExecutor
import dagr.core.tasksystem.Task.{TaskInfo => RootTaskInfo}
import dagr.core.tasksystem._
import org.scalatest.PrivateMethodTester

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

/** A place to store methods that call private methods in [[GraphExecutorImpl]]. */
private object GraphExecutorImplTest extends PrivateMethodTester {

  private val processTaskDecorate = PrivateMethod[Future[Task]]('processTask)
  def processTask(graphExecutor: GraphExecutorImpl[UnitTask], task: Task): Future[Task] = {
    graphExecutor invokePrivate processTaskDecorate(task)
  }

  private val buildAndExecuteDecorate = PrivateMethod[Future[Task]]('buildAndExecute)
  def buildAndExecute(graphExecutor: GraphExecutorImpl[UnitTask], task: Task): Future[Task] = {
    graphExecutor invokePrivate buildAndExecuteDecorate(task)
  }

  private val buildTaskDecorate = PrivateMethod[Future[Seq[Task]]]('buildTask)
  def buildTask(graphExecutor: GraphExecutorImpl[UnitTask], task: Task): Future[Seq[Task]] = {
    graphExecutor invokePrivate buildTaskDecorate(task)
  }

  private val executeWithTaskExecutorDecorate = PrivateMethod[Future[UnitTask]]('executeWithTaskExecutor)
  def executeWithTaskExecutor(graphExecutor: GraphExecutorImpl[UnitTask], task: Task): Future[UnitTask] = {
    graphExecutor invokePrivate executeWithTaskExecutorDecorate(task)
  }

  private val executeMultiTaskDecorate = PrivateMethod[Future[Task]]('executeMultiTask)
  def executeMultiTask(graphExecutor: GraphExecutorImpl[UnitTask], task: Task, childTasks: Seq[Task]): Future[Task] = {
    graphExecutor invokePrivate executeMultiTaskDecorate(task, childTasks)
  }

  private val submissionFutureDecorate = PrivateMethod[Future[Future[UnitTask]]]('submissionFuture)
  def submissionFuture(graphExecutor: GraphExecutorImpl[UnitTask], task: Task): Future[Future[UnitTask]] = {
    graphExecutor invokePrivate submissionFutureDecorate(task)
  }

  private val executionFutureDecorate = PrivateMethod[Future[UnitTask]]('executionFuture)
  def executionFuture(graphExecutor: GraphExecutorImpl[UnitTask], task: Task, subFuture: Future[Future[UnitTask]]): Future[UnitTask] = {
    graphExecutor invokePrivate executionFutureDecorate(task, subFuture)
  }

  private val onCompleteFutureDecorate = PrivateMethod[Future[UnitTask]]('onCompleteFuture)
  def onCompleteFuture(graphExecutor: GraphExecutorImpl[UnitTask], task: Task, execFuture: Future[UnitTask]): Future[UnitTask] = {
    graphExecutor invokePrivate onCompleteFutureDecorate(task, execFuture)
  }

  private val completedTaskFutureDecorate = PrivateMethod[Future[UnitTask]]('completedTaskFuture)
  def completedTaskFuture(graphExecutor: GraphExecutorImpl[UnitTask], task: Task, onComplFuture: Future[UnitTask]): Future[UnitTask] = {
    graphExecutor invokePrivate completedTaskFutureDecorate(task, onComplFuture)
  }

  private val requireNoDependenciesDecorate = PrivateMethod[Unit]('requireNoDependencies)
  def requireNoDependencies(graphExecutor: GraphExecutorImpl[UnitTask], task: Task, missingOk: Boolean): Unit = {
    graphExecutor invokePrivate requireNoDependenciesDecorate(task, missingOk)
  }

  private val updateMetadataDecorate = PrivateMethod[RootTaskInfo]('updateMetadata)
  def updateMetadata(graphExecutor: GraphExecutorImpl[UnitTask], task: Task, status: TaskStatus): RootTaskInfo = {
    graphExecutor invokePrivate updateMetadataDecorate(task, status)
  }
}

class GraphExecutorImplTest extends GraphExecutorUnitSpec with PrivateMethodTester {
  import GraphExecutorImplTest._

  private def checkTaggedException[T<:Throwable](thr: Throwable, status: TaskStatus)(implicit evidence: ClassTag[T]) : Unit = {
    thr shouldBe a[TaggedException]
    val taggedException = thr.asInstanceOf[TaggedException]
    taggedException.status shouldBe status
    taggedException.thr shouldBe a[T]
  }

  "GraphExecutorImpl.tasks" should "return the list of currently tracked tasks" in {
    val graphExecutor = this.graphExecutor
    val task = successfulTask

    graphExecutor.contains(task) shouldBe false

    whenReady(Future(graphExecutor.execute(task))) { t =>
      t shouldBe 0
      graphExecutor.tasks.toSeq should contain theSameElementsInOrderAs Seq(task)
      checkStatus(graphExecutor, task, SucceededExecution)
    }
  }

  "GraphExecutorImpl.info" should "return metadata about known tasks" in {
    val graphExecutor = this.graphExecutor
    val task = successfulTask

    graphExecutor.contains(task) shouldBe false

    whenReady(Future(graphExecutor.execute(task))) { t =>
      t shouldBe 0
      checkStatus(graphExecutor, task, SucceededExecution)
    }
  }

  "GraphExecutorImpl.status" should "return the status about known tasks" in {
    val graphExecutor = this.graphExecutor
    val task = successfulTask

    graphExecutor.contains(task) shouldBe false

    whenReady(Future(graphExecutor.execute(task))) { t =>
      t shouldBe 0
      graphExecutor.contains(task) shouldBe true
      checkStatus(graphExecutor, task, SucceededExecution)
    }
  }

  "GraphExecutorImpl.execute" should "run a single task end-to-end with success" in {
    val graphExecutor = this.graphExecutor
    val root = successfulTask
    graphExecutor.execute(root) shouldBe 0
    checkStatus(graphExecutor, root, SucceededExecution)
  }

  it should "run a single task end-to-end with failure" in {
    val graphExecutor = this.graphExecutor
    val root = new ShellCommand("exit", "1") withName "exit 1"
    graphExecutor.execute(root) shouldBe 1
    checkStatus(graphExecutor, root, FailedExecution)
  }

  it should "run a pipeline end-to-end with success" in {
    val graphExecutor = this.graphExecutor
    val pipeline = this.pipeline
    graphExecutor.execute(pipeline) shouldBe 0
    checkStatus(graphExecutor, pipeline, SucceededExecution)
  }

  it should "run a pipeline end-to-end with failure" in {
    val graphExecutor = this.graphExecutor
    val pipeline = this.pipelineFailure
    graphExecutor.execute(pipeline) shouldBe 3 // pipeline and two tasks
    checkStatus(graphExecutor, pipeline, FailedExecution)
  }

  it should "fails when the task executor does not support the task" in {
    val graphExecutor = this.graphExecutor
    val root = new Task {
      final def getTasks: Traversable[_ <: this.type] = List(this)
    }
    graphExecutor.execute(root) shouldBe 1
    checkStatus(graphExecutor, root, FailedSubmission)
  }

  it should "fails when a task cannot be scheduled" in {
    val taskExecutor = new LocalTaskExecutor(scriptsDirectory=Some(scriptsDirectory), logDirectory=Some(logDirectory))
    val graphExecutor = new GraphExecutorImpl(taskExecutor=taskExecutor, dependencyGraph=DependencyGraph())
    val root = infiniteResourcesTask

    val execute = Future { graphExecutor.execute(root) }
    whenReady(execute) { result =>
      result shouldBe 1
      checkStatus(graphExecutor, root, FailedSubmission)
    }
  }

  it should "should not execute the same task twice" in {
    val root = successfulTask withName "root"
    val graphExecutor = this.graphExecutor

    whenReady(Future { graphExecutor.execute(root) }) { res1 =>
      res1 shouldBe 0
      checkStatus(graphExecutor, root, SucceededExecution)

      whenReady(Future { graphExecutor.execute(root) }) { res2 =>
        res2 shouldBe 1
        checkStatus(graphExecutor, root, FailedUnknown)
      }
    }
  }

  /** Process a given task, its sub-tree, tasks that only depend on this task.
    * 1. add any tasks that depend on this task to the dependency graph if not already added
    * 2. execute this task and it's sub-tree recursively
    * 3. wait on tasks that depend on this task that were added in #1.
    */

  "GraphExecutorImpl.processTask" should "execute with no dependent tasks" in {
    val root = successfulTask withName "root"
    val (_, graphExecutor) = graphAndExecutor(root)

    updateMetadata(graphExecutor, root, Pending)

    whenReady(processTask(graphExecutor, root)) { _ =>
      checkStatus(graphExecutor, root, SucceededExecution)
    }
  }

  it should "execute after adding a dependent task to the dependency graph" in {
    val root = successfulTask withName "root"
    val child = successfulTask withName "child"
    root ==> child
    val (dependencyGraph, graphExecutor) = graphAndExecutor(root)

    updateMetadata(graphExecutor, root, Pending)
    dependencyGraph.contains(child) shouldBe false

    whenReady(processTask(graphExecutor, root)) { _ =>
      dependencyGraph.contains(child) shouldBe true
      checkStatus(graphExecutor, root, SucceededExecution)
      checkStatus(graphExecutor, child, SucceededExecution)
    }
  }

  it should "execute after not adding a dependent task to the dependency graph since it was already added" in {
    // NB: I have no way to verify that it was actually ** not ** added
    val root = successfulTask withName "root"
    val child = successfulTask withName "child"
    root ==> child
    val (dependencyGraph, graphExecutor) = graphAndExecutor(root)
    dependencyGraph.add(child)

    updateMetadata(graphExecutor, root, Pending)
    updateMetadata(graphExecutor, child, Pending)
    dependencyGraph.contains(child) shouldBe true

    whenReady(processTask(graphExecutor, root)) { _ =>
      dependencyGraph.contains(child) shouldBe true
      checkStatus(graphExecutor, root, SucceededExecution)
      checkStatus(graphExecutor, child, SucceededExecution)
    }
  }

  it should "execute and recurse on children" in {
    val root = successfulTask withName "root"
    val middle = successfulTask withName "middle"
    val leaf = successfulTask withName "leaf"
    root ==> middle ==> leaf
    val (dependencyGraph, graphExecutor) = graphAndExecutor(root)

    updateMetadata(graphExecutor, root, Pending)
    dependencyGraph.contains(middle) shouldBe false
    dependencyGraph.contains(leaf) shouldBe false

    whenReady(processTask(graphExecutor, root)) { _ =>
      dependencyGraph.contains(middle) shouldBe true
      dependencyGraph.contains(leaf) shouldBe true

      checkStatus(graphExecutor, root, SucceededExecution)
      checkStatus(graphExecutor, middle, SucceededExecution)
      checkStatus(graphExecutor, leaf, SucceededExecution)
    }
  }

  it should "execute and recurse on children when the middle child fails" in {
    val root = successfulTask withName "root"
    val middle = failureTask withName "middle-fail"
    val leaf = failureTask withName "leaf" // use a failureTask to prove it never gets run!
    root ==> middle ==> leaf
    val (dependencyGraph, graphExecutor) = graphAndExecutor(root)

    updateMetadata(graphExecutor, root, Pending)
    dependencyGraph.contains(middle) shouldBe false
    dependencyGraph.contains(leaf) shouldBe false

    whenReady(processTask(graphExecutor, root).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
      dependencyGraph.contains(middle) shouldBe true
      dependencyGraph.contains(leaf) shouldBe true

      checkStatus(graphExecutor, root, SucceededExecution)
      checkStatus(graphExecutor, middle, FailedSubmission)
      checkStatus(graphExecutor, leaf, Pending)
    }
  }

  it should "execute and recurse on children when the leaf child fails" in {
    val root = successfulTask withName "root"
    val middle = successfulTask withName "middle"
    val leaf = failureTask withName "leaf-fail"
    root ==> middle ==> leaf
    val (dependencyGraph, graphExecutor) = graphAndExecutor(root)
    
    updateMetadata(graphExecutor, root, Pending)
    dependencyGraph.contains(middle) shouldBe false
    dependencyGraph.contains(leaf) shouldBe false

    whenReady(processTask(graphExecutor, root).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
      dependencyGraph.contains(middle) shouldBe true
      dependencyGraph.contains(leaf) shouldBe true

      checkStatus(graphExecutor, root, SucceededExecution)
      checkStatus(graphExecutor, middle, SucceededExecution)
      checkStatus(graphExecutor, leaf, FailedSubmission)
    }
  }

  it should "complete when there was a dependent task with an unmet dependency" in {
    val root = successfulTask withName "root"
    val child = successfulTask withName "middle"
    val otherRoot = successfulTask withName "middle"
    root ==> child
    otherRoot ==> child
    val (dependencyGraph, graphExecutor) = graphAndExecutor(root)
    dependencyGraph.add(otherRoot)

    updateMetadata(graphExecutor, root, Pending)
    updateMetadata(graphExecutor, otherRoot, Pending)
    dependencyGraph.contains(child) shouldBe false

    // run root, but child depends on other-root
    whenReady(processTask(graphExecutor, root)) { _ =>
      dependencyGraph.contains(root) shouldBe true
      dependencyGraph.contains(child) shouldBe true
      dependencyGraph.contains(otherRoot) shouldBe true

      checkStatus(graphExecutor, root, SucceededExecution)
      checkStatus(graphExecutor, child, Pending)
      checkStatus(graphExecutor, otherRoot, Pending)
    }

    // run other-root to release child
    whenReady(processTask(graphExecutor, otherRoot)) { _ =>
      dependencyGraph.contains(root) shouldBe true
      dependencyGraph.contains(child) shouldBe true
      dependencyGraph.contains(otherRoot) shouldBe true

      checkStatus(graphExecutor, root, SucceededExecution)
      checkStatus(graphExecutor, child, SucceededExecution)
      checkStatus(graphExecutor, otherRoot, SucceededExecution)
    }
  }

  "GraphExecutorImpl.buildAndExecute" should "build and execute a unit task successfully" in {
    val task = successfulTask
    val (_, graphExecutor) = graphAndExecutor(task)

    whenReady(buildAndExecute(graphExecutor, task)) { t =>
      checkStatus(graphExecutor, t, SucceededExecution)
    }
  }

  it should "build and execute a pipeline successfully" in {
    val pipeline = this.pipeline
    val (_, graphExecutor) = graphAndExecutor(pipeline)

    whenReady(buildAndExecute(graphExecutor, pipeline)) { t =>
      checkStatus(graphExecutor, t, SucceededExecution)
    }
  }

  it should "build and execute a pipeline to failure" in {
    val pipeline = this.pipelineFailure
    val (_, graphExecutor) = graphAndExecutor(pipeline)

    whenReady(buildAndExecute(graphExecutor, pipeline).failed) { thr: Throwable =>
      checkTaggedException[IllegalStateException](thr, FailedExecution)
    }
  }

  it should "throw an exception if the task has dependencies" in {
    val graphExecutor = this.graphExecutor
    val root = failureTask

    val child = successfulTask
    root ==> child
    updateMetadata(graphExecutor, root, Pending).status shouldBe Pending
    updateMetadata(graphExecutor, child, Pending).status shouldBe Pending
    an[IllegalArgumentException] should be thrownBy {
      Await.result(buildAndExecute(graphExecutor, child), Duration("1s"))
    }
  }

  "GraphExecutorImpl.buildTask" should "succeed if one or more tasks where built" in {
    val task = failureTask
    val (dependencyGraph, graphExecutor) = graphAndExecutor(task)

    whenReady(buildTask(graphExecutor, task)) { tasks =>
      tasks should contain theSameElementsInOrderAs Seq(task)
    }
  }

  it should "fail if no tasks were built" in {
    val graphExecutor = this.graphExecutor
    val task = new Task {
      override def getTasks = Nil
    }

    whenReady(buildTask(graphExecutor, task).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedToBuild)
    }
  }

  "GraphExecutorImpl.executeWithTaskExecutor" should "execute a task successfully end-to-end" in {
    val task = successfulTask
    val (dependencyGraph, graphExecutor) = graphAndExecutor(task)

    // scheduled and run immediately, onComplete throws
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

    whenReady(executeWithTaskExecutor(graphExecutor, task)) { t: UnitTask =>
      checkStatus(graphExecutor, t, SucceededExecution)
    }
  }

  it should "fail when any exception was thrown (not in a future)" in {
    val graphExecutor = this.graphExecutorSubmissionException
    // scheduled and run immediately
    val task = successfulTask
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

    whenReady(executeWithTaskExecutor(graphExecutor, task).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
    }
  }

  it should "fail when any future fails" in {
    {
      val graphExecutor = this.graphExecutorFailedSubmission
      // scheduled and run immediately
      val task = successfulTask
      updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

      whenReady(executeWithTaskExecutor(graphExecutor, task).failed) { thr =>
        checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
      }
    }

    {
      val graphExecutor = this.graphExecutorFailedExecution
      // scheduled and run immediately, onComplete throws
      val task = successfulTask
      updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

      whenReady(executeWithTaskExecutor(graphExecutor, task).failed) { thr =>
        checkTaggedException[IllegalArgumentException](thr, FailedExecution)
      }
    }
  }

  "GraphExecutorImpl.executeMultiTask" should "execute a pipeline successfully" in {
    val parent = this.pipeline
    val pipeline = this.pipeline
    val children = pipeline.getTasks.toSeq

    // break the link with root
    children.foreach { child =>
      pipeline.root !=> child
    }

    val (_, graphExecutor) = this.graphAndExecutor(parent)

    whenReady(executeMultiTask(graphExecutor, parent, children)) { t =>
      t shouldBe parent
      val tasks = parent +: children
      tasks.foreach { task =>
        checkStatus(graphExecutor, task, SucceededExecution)
      }
      val parentInstant = parent.taskInfo(SucceededExecution).value
      children.foreach { child =>
        val childInstant = child.taskInfo(SucceededExecution).value
        checkInstants(Seq(childInstant, parentInstant))
      }
    }
  }

  it should "execute successfully when the parent is a [[UnitTask]]" in {
    val graphExecutor = this.graphExecutor
    val parent = successfulTask
    val pipeline = this.pipeline
    val children = pipeline.getTasks.toSeq

    // break the link with root
    children.foreach { child =>
      pipeline.root !=> child
    }

    whenReady(executeMultiTask(graphExecutor, parent, children)) { t =>
      t shouldBe parent
      val tasks = parent +: children
      tasks.foreach { task =>
        checkStatus(graphExecutor, task, SucceededExecution)
      }
      val parentInstant = parent.taskInfo(SucceededExecution).value
      children.foreach { child =>
        val childInstant = child.taskInfo(SucceededExecution).value
        checkInstants(Seq(childInstant, parentInstant))
      }
    }
  }

  it should "execute a pipeline of pipelines" in {
    val graphExecutor = this.graphExecutor
    val pipeline2 = this.pipeline
    val pipeline1 = new Pipeline() {
      override def build() = root ==> pipeline2
    }
    val parent = pipeline1
    val children = parent.getTasks.toSeq

    whenReady(executeMultiTask(graphExecutor, parent, children)) { t =>
      t shouldBe pipeline1
      val tasks = Seq(parent) ++ children ++ pipeline2.tasksDependingOnThisTask
      tasks.foreach { task =>
        checkStatus(graphExecutor, task, SucceededExecution)
      }
      val parentInstant = parent.taskInfo(SucceededExecution).value
      children.foreach { child =>
        val childInstant = child.taskInfo(SucceededExecution).value
        checkInstants(Seq(childInstant, parentInstant))
      }
    }
  }

  it should "fail if any child fails" in {
    val graphExecutor = this.graphExecutor
    val pipeline2 = this.pipelineFailure withName "p2"
    val pipeline1 = new Pipeline() {
      name = "p1"
      override def build() = root ==> pipeline2
    }
    val parent = pipeline1
    val children = parent.getTasks.toSeq

    whenReady(executeMultiTask(graphExecutor, parent, children).failed) { thr: Throwable =>
      checkTaggedException[IllegalStateException](thr, FailedExecution)

      val okTasks = Seq(pipeline2.okTasks.head)
      okTasks.foreach { task =>
        checkStatus(graphExecutor, task, SucceededExecution)
      }

      val failTasks = Seq(pipeline2.failTask, pipeline2, pipeline1)
      val failInstants = failTasks.map { task =>
        checkStatus(graphExecutor, task, FailedExecution)
        task.taskInfo(FailedExecution).value
      }
      checkInstants(failInstants)

      checkStatus(graphExecutor, pipeline2.okTasks.last, Pending)

      // NB: pipeline2.okTasks(1) may have completed
      Seq(SucceededExecution, Running, Submitted, Queued) should contain (pipeline2.okTasks(1).taskInfo.status)
    }
  }

  it should "fail if any child is already part of the dependency graph" in {
    val root = successfulTask withName "root"
    val child = successfulTask withName "child"

    root ==> child

    val (dependencyGraph, graphExecutor) = this.graphAndExecutor(root)
    updateMetadata(graphExecutor, root, Running)

    // naughty
    dependencyGraph.add(child)
    updateMetadata(graphExecutor, child, Pending)

    whenReady(executeMultiTask(graphExecutor, root, Seq(child)).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedExecution)

      checkStatus(graphExecutor, child, Pending)
      checkStatus(graphExecutor, root, FailedExecution)
    }
  }

  "GraphExecutorImpl.submissionFuture" should "throw an exception if the task status was not Queued" in {
    val graphExecutor = this.graphExecutor
    val task = infiniteResourcesTask

    an[NoSuchElementException] should be thrownBy submissionFuture(graphExecutor, task)

    updateMetadata(graphExecutor, task, Pending).status shouldBe Pending
    an[IllegalArgumentException] should be thrownBy submissionFuture(graphExecutor, task)
  }

  it should "fail if TaskExecutor.execute throws an exception" in {
    // A task executor that throws an exception
    val graphExecutor = graphExecutorSubmissionException
    val task = infiniteDurationTask
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

    whenReady(submissionFuture(graphExecutor, task).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
    }
  }


  it should "fail if the task could not be submitted" in {
    // A task executor that fails the outer future execute method
    val graphExecutor = graphExecutorFailedSubmission
    val task = infiniteDurationTask
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

    whenReady(submissionFuture(graphExecutor, task).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
    }
  }

  it should "fails when a task cannot be scheduled" in {
    val taskExecutor = new LocalTaskExecutor(scriptsDirectory=Some(scriptsDirectory), logDirectory=Some(logDirectory))
    val graphExecutor = new GraphExecutorImpl(taskExecutor=taskExecutor, dependencyGraph=DependencyGraph())
    val root = infiniteResourcesTask

    updateMetadata(graphExecutor, root, Queued).status shouldBe Queued

    whenReady(submissionFuture(graphExecutor, root).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
    }
  }

  it should "have status FailedExecution when the task fails execution" in {
    val graphExecutor = graphExecutorFailedExecution
    val task = infiniteResourcesTask // NB: infinite resources, cannot be scheduled
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

    whenReady(submissionFuture(graphExecutor, task)) { execFuture =>
      checkStatus(graphExecutor, task, Submitted)
      whenReady(execFuture.failed) { thr =>
        checkStatus(graphExecutor, task, Submitted)
      }
    }
  }

  it should "have status FailedSubmission when the task cannot be scheduled" in {
    val graphExecutor = this.graphExecutor
    val task = infiniteResourcesTask // NB: infinite resources
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

    whenReady(submissionFuture(graphExecutor, task).failed) { thr =>
      checkTaggedException[IllegalArgumentException](thr=thr, status=FailedSubmission)
      checkStatus(graphExecutor, task, FailedSubmission)
    }
  }

  it should "have status Submitted when the task can be scheduled (but not yet running)" in {
    val graphExecutor = this.graphExecutor
    val task = infiniteDurationTask // NB: no resources
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

    whenReady(submissionFuture(graphExecutor, task)) { execFuture =>
      execFuture.isCompleted shouldBe false
      checkStatus(graphExecutor, task, Submitted)
    }
  }

  "GraphExecutorImpl.executionFuture" should "return a failure if the submission future failed" in {
    // A task executor that fails the inner future execute method
    val graphExecutor = graphExecutorFailedSubmission
    val task = infiniteDurationTask
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

    val subFuture = submissionFuture(graphExecutor, task)
    val execFuture = executionFuture(graphExecutor, task, subFuture)

    whenReady(execFuture.failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedSubmission)
    }
  }

  it should "return a failure if the execution future failed" in {
    // A task executor that fails the outer future execute method
    val graphExecutor = graphExecutorFailedExecution
    val task = infiniteDurationTask
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

    val subFuture = submissionFuture(graphExecutor, task)
    val execFuture = executionFuture(graphExecutor, task, subFuture)

    whenReady(execFuture.failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedExecution)
    }
  }

  it should "throw an exception if the status was not Submitted" in {
    val graphExecutor = this.graphExecutor
    val task = infiniteDurationTask // scheduled immediately
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

    val subFuture = submissionFuture(graphExecutor, task)

    whenReady(subFuture) { _ =>
      // Update to Queued
      updateMetadata(graphExecutor, task, Queued).status shouldBe Queued
    }
    val execFuture = executionFuture(graphExecutor, task, subFuture)

    an[IllegalArgumentException] should be thrownBy {
      Await.result(execFuture, Duration("1s"))
    }
  }

  it should "have status Running while running" in {
    val graphExecutor = this.graphExecutor
    val task = infiniteDurationTask // scheduled immediately
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

    val subFuture = submissionFuture(graphExecutor, task)
    val execFuture = executionFuture(graphExecutor, task, subFuture)

    // wait until it is scheduled
    whenReady(subFuture) { _ => }

    checkStatus(graphExecutor, task, Running)
    execFuture.isCompleted shouldBe false
  }


  it should "have status Running after executing" in {
    val graphExecutor = this.graphExecutor
    val task = successfulTask // scheduled and run immediately
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

    val subFuture = submissionFuture(graphExecutor, task)

    // wait until it is scheduled
    whenReady(subFuture) { _ =>
      checkStatus(graphExecutor, task, Submitted)
      val execFuture = executionFuture(graphExecutor, task, subFuture)
      whenReady(execFuture) { t =>
        checkStatus(graphExecutor, t, Running)
      }
    }
  }

  "GraphExecutorImpl.onCompleteFuture" should "throw an exception if the status was not running" in {
    val graphExecutor = this.graphExecutor
    val task = successfulTask // scheduled and run immediately
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

    val subFuture = submissionFuture(graphExecutor, task)
    val execFuture = executionFuture(graphExecutor, task, subFuture)

    whenReady(execFuture) { _ =>
      updateMetadata(graphExecutor, task, Queued).status shouldBe Queued
    }

    an[IllegalArgumentException] should be thrownBy {
      Await.result(onCompleteFuture(graphExecutor, task, execFuture), Duration("1s"))
    }
  }

  it should "fail when the onComplete method returns false" in {
    val graphExecutor = this.graphExecutor
    // scheduled and run immediately, onComplete throws
    val task = new PromiseTask(Duration.Zero, ResourceSet.empty) with UnitTask {
      override def onComplete(exitCode: Int): Boolean = false
    }

    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

    val subFuture = submissionFuture(graphExecutor, task)
    val execFuture = executionFuture(graphExecutor, task, subFuture)
    val onComplFuture = onCompleteFuture(graphExecutor, task, execFuture=execFuture)

    whenReady(onComplFuture.failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedOnComplete)
    }
  }

  it should "fail when the execFuture has failed" in {
    val graphExecutor = this.graphExecutor
    // scheduled and run immediately, onComplete throws
    val task = new PromiseTask(Duration.Zero, ResourceSet.empty) with UnitTask {
      override def onComplete(exitCode: Int): Boolean = false
    }

    updateMetadata(graphExecutor, task, Running).status shouldBe Running

    val execFuture: Future[UnitTask] = Future.failed(TaggedException(thr=new IllegalArgumentException, status=FailedExecution))
    val onComplFuture = onCompleteFuture(graphExecutor, task, execFuture=execFuture)

    whenReady(onComplFuture.failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedExecution)
    }
  }

  it should "succeed when the onComplete method returns true" in {
    val graphExecutor = this.graphExecutor
    // scheduled and run immediately, onComplete throws
    val task = successfulTask
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued

    val subFuture = submissionFuture(graphExecutor, task)
    val execFuture = executionFuture(graphExecutor, task, subFuture)
    val onComplFuture = onCompleteFuture(graphExecutor, task, execFuture=execFuture)

    whenReady(onComplFuture) { t =>
      checkStatus(graphExecutor, t, Running)
    }
  }

  "GraphExecutorImpl.completedTaskFuture" should "fail unknown if task could not be removed from the task executor" in {
    val graphExecutor = {
      // an executor that adds the task info back in before returning the future from execute()
      val taskExecutor = new LocalTaskExecutor(scriptsDirectory=Some(scriptsDirectory), logDirectory=Some(logDirectory)) {
        override def execute(task: UnitTask): Future[Future[UnitTask]] = {
          val future = super.execute(task)
          val info = this.taskInfo(task)
          // add the task info back in before completing!
          future map { execFuture =>
             execFuture map { t => this.taskInfo(task) = info; t }
           }
        }
        override def throwableIfCanNeverBeScheduled(task: UnitTask): Option[Throwable] = None
      }
      new GraphExecutorImpl(taskExecutor=taskExecutor, dependencyGraph=DependencyGraph())
    }
    // scheduled and runs immediately
    val task = successfulTask
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued
    val subFuture = submissionFuture(graphExecutor, task)
    val execFuture = executionFuture(graphExecutor, task, subFuture)
    val onComplFuture = onCompleteFuture(graphExecutor, task, execFuture=execFuture)

    whenReady(completedTaskFuture(graphExecutor, task, onComplFuture=onComplFuture).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedUnknown)
    }
  }

  it should "throw an exception if the throwable is not either Failed or Running" in {
    val graphExecutor = this.graphExecutor
    // scheduled immediately, but runs forever
    val task = infiniteDurationTask
    val onComplFuture = Future.failed(TaggedException(thr=new IllegalArgumentException, status=Pending))
    an[IllegalArgumentException] should be thrownBy {
      Await.result(completedTaskFuture(graphExecutor, task, onComplFuture=onComplFuture), Duration("1s"))
    }
  }

  it should "update to the failed status when the the future failed" in {
    val graphExecutor = this.graphExecutor
    val task = successfulTask
    val onComplFuture = Future.failed(TaggedException(thr=new IllegalArgumentException, status=FailedOnComplete))
    whenReady(completedTaskFuture(graphExecutor, task, onComplFuture=onComplFuture).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedOnComplete)
    }
  }

  it should "fail with the throwable if it is not a TaggedException, and update the status to FailedUnknown" in {
    val graphExecutor = this.graphExecutor
    val task = successfulTask
    val onComplFuture = Future.failed(new IllegalArgumentException)
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued
    whenReady(completedTaskFuture(graphExecutor, task, onComplFuture=onComplFuture).failed) { thr: Throwable =>
      checkTaggedException[IllegalArgumentException](thr, FailedUnknown)
    }
  }

  it should "complete successfully and update the status to SucceededExecution" in {
    val graphExecutor = this.graphExecutor
    val task = successfulTask
    val onComplFuture = Future.successful(task)
    updateMetadata(graphExecutor, task, Queued).status shouldBe Queued
    whenReady(completedTaskFuture(graphExecutor, task, onComplFuture=onComplFuture)) { t =>
      checkStatus(graphExecutor, task, SucceededExecution)
    }
  }

  "GraphExecutorImpl.requireNoDependencies" should "should throw an exception if the task is not tracked and missingOk is false" in {
    val graphExecutor = this.graphExecutor
    val task = successfulTask
    an[IllegalArgumentException] should be thrownBy requireNoDependencies(graphExecutor, task, missingOk=false)
  }

  it should "should not throw an exception if the task is not tracked and missingOk is true" in {
    val graphExecutor = this.graphExecutor
    val task = successfulTask
    requireNoDependencies(graphExecutor, task, missingOk=true)
  }

  it should "should throw an exception only if the task has dependencies" in {
    val root  = successfulTask
    val child = successfulTask
    val (_, graphExecutor) = this.graphAndExecutor(root)

    root ==> child
    requireNoDependencies(graphExecutor, root, missingOk=false)
    an[IllegalArgumentException] should be thrownBy requireNoDependencies(graphExecutor, child, missingOk=false)
  }

  // TODO: test that onComplete runs when it fails or succeeds

}
