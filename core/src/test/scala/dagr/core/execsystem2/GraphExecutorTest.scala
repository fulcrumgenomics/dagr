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

import dagr.core.TestTags
import dagr.core.exec.{Cores, Memory, ResourceSet}
import dagr.core.execsystem.SystemResources
import dagr.core.execsystem2.TaskStatus._
import dagr.core.execsystem2.local.LocalTaskExecutor
import dagr.core.tasksystem.Task.{TaskInfo => RootTaskInfo}
import dagr.core.tasksystem.{Retry, _}
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.{Seconds, Span}

import scala.concurrent.Future
import scala.concurrent.duration.Duration

class GraphExecutorTest extends GraphExecutorUnitSpec {

  ////////////////////////////////////////////////////////////////////////////////
  // Basic tests
  ////////////////////////////////////////////////////////////////////////////////

  "GraphExecutor" should "run a single task end-to-end with success" in {
    val graphExecutor = this.graphExecutor
    val root = successfulTask withName "p1"
    graphExecutor.execute(root) shouldBe 0
    checkStatus(graphExecutor, root, SucceededExecution)
  }

  it should "run a single task end-to-end with failure" in {
    val graphExecutor = this.graphExecutor
    val root = new ShellCommand("exit", "1") withName "exit 1"
    graphExecutor.execute(root) shouldBe 1
    checkStatus(graphExecutor, root, FailedExecution)
  }

  it should "run two tasks end-to-end with success" in {
    val graphExecutor = this.graphExecutor
    val root = successfulTask withName "p1"
    val child = successfulTask withName "p2"

    root ==> child

    graphExecutor.execute(root) shouldBe 0
    checkStatus(graphExecutor, root, SucceededExecution)
    checkStatus(graphExecutor, child, SucceededExecution)
  }

  it should "run a few tasks end-to-end with success" in {
    val graphExecutor = this.graphExecutor
    val root = successfulTask withName "root"
    val left = successfulTask withName "left"
    val right = successfulTask withName "right"
    val leaf = successfulTask withName "leaf"

    root ==> (left :: right) ==> leaf

    graphExecutor.execute(root) shouldBe 0
    Seq(root, left, right, leaf).foreach { t => checkStatus(graphExecutor, t, SucceededExecution) }
  }

  it should "run a pipeline end-to-end with success" in {
    val graphExecutor = this.graphExecutor
    val pipeline = this.pipeline
    graphExecutor.execute(pipeline) shouldBe 0
    checkStatus(graphExecutor, pipeline, SucceededExecution)
  }

  it should "run a pipeline end-to-end with failure" in {
    val graphExecutor = this.graphExecutor
    val pipeline = this.pipelineFailure // pipeline ==> (okTasks(0) ==> (failTask :: okTasks(1)) ==> okTasks(2))
    graphExecutor.execute(pipeline) shouldBe 3
    checkPipelineFailure(pipeline, graphExecutor)
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

  it should "fails when a task is executed that has dependencies" in {
    val graphExecutor = this.graphExecutor
    val root = successfulTask
    val child = successfulTask
    root ==> child

    graphExecutor.execute(child) shouldBe 1
    //an[IllegalArgumentException] should be thrownBy graphExecutor.execute(child)
  }

  it should "not depend on the order tasks are enqueued" in {
    val graphExecutor = this.graphExecutor
    val task = new Task {
      override def getTasks: Traversable[_ <: Task] = {
        pipeline.getTasks.toList.reverse
      }
    }

    graphExecutor.execute(task) shouldBe 0
    checkInfo(graphExecutor, task=task, statuses=Seq(Pending, Queued, Running, SucceededExecution))
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Checking various routes for a single task
  //   * FailedUnknown
  //   * Pending => Queued => Submitted => Running => SucceededExecution (for executable tasks)
  //   * Pending => Queued => Running => SucceededExecution (for pipelines)
  //   * Pending => FailedUnknown
  //   * Pending => Queued => FailedToBuild
  //   * Pending => Queued => Submitted => FailedSubmission
  //   * Pending => Queued => Submitted => Running => FailedExecution
  //   * Pending => Queued => Submitted => Running => FailedOnComplete
  // *** Stopped and ManuallySucceeded not tested ***
  ////////////////////////////////////////////////////////////////////////////////

  it should "route through FailedUnknown" in {
    val graphExecutor = this.graphExecutor
    // fails when we look at dependencies
    val root = new PromiseTask(Duration.Zero, ResourceSet.empty) with UnitTask {
      var count = true
      override def tasksDependedOn: Traversable[Task] = throw new IllegalArgumentException
    } withName "root"
    graphExecutor.execute(root) shouldBe 1

    checkInfo(graphExecutor, task=root, statuses=Seq(FailedUnknown))
  }

  it should "route through Pending => Queued => Submitted => Running => SucceededExecution (for executable tasks)" in {
    val graphExecutor = this.graphExecutor
    val root = successfulTask
    graphExecutor.execute(root) shouldBe 0

    checkInfo(graphExecutor, task=root, statuses=Seq(Pending, Queued, Submitted, Running, SucceededExecution))
  }

  it should "route through Pending => Queued => Running => SucceededExecution (for pipelines)" in {
    val graphExecutor = this.graphExecutor
    val task = pipeline
    graphExecutor.execute(task) shouldBe 0

    checkInfo(graphExecutor, task=task, statuses=Seq(Pending, Queued, Running, SucceededExecution))
  }

  // This test is disabled since it relies on how many times `tasksDependedOn` gets called, which is a bad idea.  I
  // don't see another way to make it blow up :/
  /*
  it should "route through Pending => FailedUnknown" in {
    val graphExecutor = this.graphExecutor
    // fails when we look at dependencies the second time!
    val root = new PromiseTask(Duration.Zero, ResourceSet.empty) with UnitTask {
      var count = true
      override def tasksDependedOn: Traversable[Task] = {
        if (first) {
          first = false
          Traversable.empty
        }
        else {
          throw new IllegalArgumentException
        }
      }
    } withName "root"
    graphExecutor.execute(root) shouldBe 1
    checkInfo(graphExecutor, task=root, statuses=Seq(Pending, FailedUnknown))
  }
  */

  it should "route through Pending => Queued => FailedToBuild" in {
    val graphExecutor = this.graphExecutor
    // fails when building
    val root = new Task {
      withName("root")
      override def getTasks = Nil
    }

    graphExecutor.execute(root) shouldBe 1
    checkInfo(graphExecutor, task=root, statuses=Seq(Pending, Queued, FailedToBuild))
  }

  it should "route through Pending => Queued => Submitted => FailedSubmission" in {
    val graphExecutor = this.graphExecutor
    val root = new Task {
      final def getTasks: Traversable[_ <: this.type] = List(this)
    }

    graphExecutor.execute(root) shouldBe 1
    checkInfo(graphExecutor, task=root, statuses=Seq(Pending, Queued, Submitted, FailedSubmission))
  }

  it should "route through Pending => Queued => Submitted => Running => FailedExecution" in {
    val graphExecutor = this.graphExecutor
    val root = new ShellCommand("exit", "1") withName "exit 1"

    graphExecutor.execute(root) shouldBe 1
    checkInfo(graphExecutor, task=root, statuses=Seq(Pending, Queued, Submitted, Running, FailedExecution))
  }

  it should "route through Pending => Queued => Submitted => Running => FailedOnComplete" in {
    val graphExecutor = this.graphExecutor
    // scheduled and run immediately, onComplete throws
    val root = new PromiseTask(Duration.Zero, ResourceSet.empty) with UnitTask {
      override def onComplete(exitCode: Int): Boolean = false
    }

    graphExecutor.execute(root) shouldBe 1
    checkInfo(graphExecutor, task=root, statuses=Seq(Pending, Queued, Submitted, Running, FailedOnComplete))
  }

  ////////////////////////////////////////////////////////////////////////////////
  // More complicated sets of tasks
  ////////////////////////////////////////////////////////////////////////////////

  it should "execute a chain of one-task pipelines to successful execution" in {
    val graphExecutor = this.graphExecutor
    val p1 = pipelineOneTask withName "p1"
    val p2 = pipelineOneTask withName "p2"
    val p3 = pipelineOneTask withName "p3"
    p1 ==> p2  ==> p3

    graphExecutor.execute(p1) shouldBe 0

    Seq(p1, p2, p3).foreach { p =>
      checkInfo(graphExecutor, task=p, statuses=Seq(Pending, Queued, Running, SucceededExecution))
    }

    val times = Seq(p1, p2, p3).map(_.taskInfo(SucceededExecution).value)
    checkInstants(times)
  }

  it should "execute a chain of pipelines, each with multiple tasks, to successful execution" in {
    val graphExecutor = this.graphExecutor
    val p1 = pipeline withName "p1"
    val p2 = pipeline withName "p2"
    val p3 = pipeline withName "p3"
    p1 ==> p2 ==> p3

    graphExecutor.execute(p1) shouldBe 0

    Seq(p1, p2, p3).foreach { p =>
      checkInfo(graphExecutor, p, statuses=Seq(Pending, Queued, Running, SucceededExecution))
    }
    val times = Seq(p1, p2, p3).map(_.taskInfo(SucceededExecution).value)
    checkInstants(times)
  }

  it should "succeed a pipeline task that has a dependent task that will fail" in {
    val graphExecutor = this.graphExecutor
    val p1 = this.pipelineOneTask withName "p1"
    val fail = failureTask

    p1 ==> fail
    graphExecutor.execute(p1) shouldBe 1 // fail

    checkInfo(graphExecutor, p1, statuses=Seq(Pending, Queued, Running, SucceededExecution))
    checkInfo(graphExecutor, fail, statuses=Seq(Pending, Queued, Submitted, FailedSubmission))
  }

  it should "execute a chain of pipelines where an intermediate pipeline fails" in {
    val graphExecutor = this.graphExecutor
    val p1 = pipeline withName "p1"
    val p2 = pipelineFailure withName "p2"
    val p3 = pipeline withName "p3"
    p1 ==> p2 ==> p3

    val result = graphExecutor.execute(p1)

    result should be >= 4 // p1, p2, p2-1, p2-fail, p3
    result should be <= 5 // p1, p2, p2-1, p2-2, p2-fail, p3

    // success
    checkInfo(graphExecutor, p1, statuses=Seq(Pending, Queued, Running, SucceededExecution))

    // failure
    checkPipelineFailure(p2, graphExecutor)

    // failure
    checkStatus(graphExecutor, p3, Pending)
  }


  ////////////////////////////////////////////////////////////////////////////////
  // Retries
  ////////////////////////////////////////////////////////////////////////////////

  it should "retry a task that succeeds on its second attempt" in {
    val graphExecutor = this.graphExecutor
    val task = new SimpleInJvmTask with Retry {
      var attempt = 0
      def run(): Unit = { attempt += 1; require(attempt > 1) }
      override def retry(resources: SystemResources, taskInfo: RootTaskInfo): Boolean = attempt < 2
    } withName "retry-task"

    graphExecutor.execute(task) shouldBe 0
    val statuses = Seq(Pending, Queued, Submitted, Running, FailedExecution, Queued, Submitted, Running, SucceededExecution)
    checkInfo(graphExecutor, task, statuses=statuses, attempts=2)
  }

  it should "retry a task that fails on its second and final attempt" in {
    val graphExecutor = this.graphExecutor
    val task = new SimpleInJvmTask with Retry {
      var attempt = 0
      def run(): Unit = { attempt += 1; throw new IllegalArgumentException("this task should never succeed") }
      override def retry(resources: SystemResources, taskInfo: RootTaskInfo): Boolean = attempt < 2
    } withName "retry-failure-task"

    graphExecutor.execute(task) shouldBe 1
    val statuses = Seq(Pending, Queued, Submitted, Running, FailedExecution, Queued, Submitted, Running, FailedExecution)
    checkInfo(graphExecutor, task, statuses=statuses, attempts=2)
  }

  it should "retry a task that succeeds on its second attempt, after which a dependent task runs" in {
    val graphExecutor = this.graphExecutor
    val root = new SimpleInJvmTask with Retry {
      var attempt = 0
      def run(): Unit = { attempt += 1; require(attempt > 1) }
      override def retry(resources: SystemResources, taskInfo: RootTaskInfo): Boolean = attempt < 2
    } withName "retry-task"
    val child = successfulTask

    root ==> child

    graphExecutor.execute(root) shouldBe 0
    Seq(root, child).foreach { task =>
      val attempts = if (root == task) 2 else 1
      val statuses = if (root == task) {
        Seq(Pending, Queued, Submitted, Running, FailedExecution, Queued, Submitted, Running, SucceededExecution)
      }
      else {
        Seq(Pending, Queued, Submitted, Running, SucceededExecution)
      }
      checkInfo(graphExecutor, task, statuses=statuses, attempts=attempts)
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // Long running tests
  ////////////////////////////////////////////////////////////////////////////////

  {
    val numTasks = 10000
    val dependencyProbability = 0.1

    trait ZTask extends UnitTask {
      override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
        val mem = Memory("1g")
        (8 to 1 by -1).map(c => ResourceSet(Cores(c), mem)).find(rs => availableResources.subset(rs).isDefined)
      }
    }

    class ATask extends ProcessTask with ZTask {
      override def args = "exit" :: "0" :: Nil
    }

    class BTask extends SimpleInJvmTask with ZTask {
      def run(): Unit = Unit
    }

    def toATask: () => ATask = () => new ATask
    def toBTask: () => BTask = () => new BTask


    Seq(true, false).foreach { inJvm: Boolean =>

      val toTask   = if (inJvm) toBTask else toATask
      val taskType = if (inJvm) "JVM" else "Shell"

      it should s"handle a few thousand $taskType tasks" taggedAs TestTags.LongRunningTest in {

        // create the tasks
        val root = successfulTask withName "root"
        val tasks: Seq[Task] = for (i <- 1 to numTasks) yield (toTask() withName s"task-$i")

        // make them depend on previous tasks
        val randomNumberGenerator = scala.util.Random
        for (i <- 1 until numTasks) {
          for (j <- 1 until i) {
            if (randomNumberGenerator.nextFloat < dependencyProbability) tasks(j) ==> tasks(i)
          }
        }

        val rootTasks: Seq[Task] = tasks.last +: tasks.filter(_.tasksDependedOn.isEmpty)
        rootTasks.foreach { task =>
          root ==> task
        }

        //val systemResources = SystemResources(cores=Cores(16), systemMemory=Memory("16g"), jvmMemory=Memory("16g"))
        val systemResources = SystemResources.infinite
        val taskExecutor    = new LocalTaskExecutor(scriptsDirectory=Some(scriptsDirectory), logDirectory=Some(logDirectory), systemResources=systemResources)
        val graphExecutor   = new GraphExecutorImpl(taskExecutor=taskExecutor, dependencyGraph=DependencyGraph())

        whenReady(Future { graphExecutor.execute(root) }, timeout=Timeout(Span(120, Seconds))) { t =>
          t shouldBe 0
          (Seq(root) ++ tasks).foreach { task =>
            //println(s"checking status for ${task.name}")
            checkStatus(graphExecutor, task, SucceededExecution)
          }
        }
      }
    }
  }

  // TODO: add relevant tests from TaskManagerTest
  // Execution
  // - handle a few thousand tasks
  // - set the submission, start, and end dates correctly for Pipelines
  // - get the submission, start, and end dates correctly for a Pipeline within a Pipeline
  // Cycles
  // - detect a task that has cycles in in its dependencies
  // - detect a cycle in the graph introduced by pipeline.build()
  // Retries
  // - retry a task once and its attempt index is updated; the task succeeds the second time
  // - retry a task N times and it succeeds on attempt 1 < M <= N
  // - retry a task N times and it fails all attempts
  // - the onComplete method should always be run!!!
  // - run a task that fails its onComplete method, is retried, where it modifies the onComplete method return value, and succeeds
  // - run a task that fails its onComplete method, whereby it changes its args to empty, and succeeds
  // - run a task, that its onComplete method mutates its args and return value based on the attempt index
  //

}
