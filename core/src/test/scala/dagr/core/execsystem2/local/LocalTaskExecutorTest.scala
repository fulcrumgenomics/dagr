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

package dagr.core.execsystem2.local

import java.nio.file.Files

import com.fulcrumgenomics.commons.CommonsDef.DirPath
import dagr.core.FutureUnitSpec
import dagr.api.models.util.{Cores, ResourceSet}
import dagr.core.execsystem2.TaskStatus._
import dagr.core.execsystem2._
import dagr.core.tasksystem._

import scala.concurrent.duration.Duration
import scala.concurrent.{CancellationException, Future}

class LocalTaskExecutorTest extends FutureUnitSpec {

  private def scriptDirectory: DirPath = {
    val dir = Files.createTempDirectory("LocalTaskExecutorTest.scripts")
    dir.toFile.deleteOnExit()
    dir
  }

  private def logDirectory: DirPath = {
    val dir = Files.createTempDirectory("LocalTaskExecutorTest.scripts")
    dir.toFile.deleteOnExit()
    dir
  }

  private def executor = new LocalTaskExecutor(scriptDirectory=scriptDirectory, logDirectory=logDirectory)

  private def info(task: UnitTask): task.type = {
    new TaskInfo(task=task, initStatus=Queued)
    task
  }

  "LocalTaskExecutor.execute" should "return a failure if the task cannot be scheduled (not enough resources)" in {
    val executor = this.executor
    val task = info(PromiseTask())
    val future: Future[Future[UnitTask]] = executor.execute(task)

    whenReady(future.failed) { t =>
      executor.contains(task) shouldBe false
    }
  }

  it should "run a task to completion" in {
    val executor = this.executor
    val task: PromiseTask = info(PromiseTask(resourceSet=ResourceSet.empty))
    val future: Future[Future[UnitTask]] = executor.execute(task)

    whenReady(future) { execFuture: Future[UnitTask] =>
      // executing but waiting on the promise to complete
      executor.contains(task) shouldBe true
      executor.waiting(task) shouldBe false
      executor.running(task) shouldBe true

      // complete the promise
      task.promise.success(42)

      whenReady(execFuture) { t =>
        executor.contains(t) shouldBe false
      }
    }
  }

  it should "run a task to completion a second time" in {
    val executor = this.executor
    val task: PromiseTask = info(PromiseTask(duration=Duration.Zero, resourceSet=ResourceSet.empty))
    val future: Future[Future[UnitTask]] = executor.execute(task)

    whenReady(future) { execFuture: Future[UnitTask] =>
      whenReady(execFuture) { t =>
        executor.contains(t) shouldBe false
      }
    }
  }

  "LocalTaskExecutor.kill" should "kill task that is not tracked and return None" in {
    this.executor.kill(PromiseTask()) shouldBe None
  }

  it should "kill task that has not been scheduled and return Some(false)" in {
    val executor = this.executor

    val allTheMemory = ResourceSet(Cores(1), LocalTaskExecutorDefaults.defaultSystemResources.jvmMemory)

    // this task takes all the resources
    val blockingTask: PromiseTask = info(PromiseTask(
      duration=Duration.Inf,
      resourceSet=allTheMemory
    )) withName "blockingTask"

    // this task takes one core, but depends on the first
    val child: PromiseTask = info(PromiseTask(
      duration=Duration.Zero,
      resourceSet=allTheMemory
    )) withName "child"

    val blockingFuture = executor.execute(blockingTask)
    val childFuture = executor.execute(child)

    whenReady(blockingFuture) { execFuture =>

      // executing but waiting on the promise to complete
      executor.contains(blockingTask) shouldBe true
      executor.waiting(blockingTask) shouldBe false
      executor.running(blockingTask) shouldBe true

      // not yet scheduled
      childFuture.isCompleted shouldBe false
      executor.contains(child) shouldBe true
      executor.waiting(child) shouldBe true
      executor.running(child) shouldBe false

      // kill the child task
      executor.kill(child).value shouldBe false
      executor.contains(child) shouldBe false

      whenReady(childFuture.failed) { thr: Throwable =>
        thr shouldBe a[CancellationException]
      }

      // complete the promise
      blockingTask.promise.success(42)

      whenReady(execFuture) { t =>
        executor.contains(t) shouldBe false
      }
    }
  }

  it should "kill a running task and return Some(true)" in {
    val executor = this.executor
    val task: PromiseTask = info(PromiseTask(resourceSet=ResourceSet.empty))
    val future: Future[Future[UnitTask]] = executor.execute(task)

    whenReady(future) { _ =>
      // executing but waiting on the promise to complete
      executor.contains(task) shouldBe true
      executor.waiting(task) shouldBe false
      executor.running(task) shouldBe true

      executor.kill(task).value shouldBe true
    }
  }

  it should "kill a completed task and return None" in {
    val executor = this.executor
    val task: PromiseTask = info(PromiseTask(resourceSet=ResourceSet.empty))
    val future: Future[Future[UnitTask]] = executor.execute(task)

    whenReady(future) { execFuture: Future[UnitTask] =>
      // executing but waiting on the promise to complete
      executor.contains(task) shouldBe true
      executor.waiting(task) shouldBe false
      executor.running(task) shouldBe true

      // complete the promise
      task.promise.success(42)

      whenReady(execFuture) { t: UnitTask =>
        executor.kill(task) shouldBe None
        executor.contains(t) shouldBe false
      }
    }
  }

  it should "kill a completed task and return None a second time" in {
    val executor = this.executor
    val task: PromiseTask = info(PromiseTask(duration=Duration.Zero, resourceSet=ResourceSet.empty))
    val future: Future[Future[UnitTask]] = executor.execute(task)

    whenReady(future) { execFuture: Future[UnitTask] =>
      whenReady(execFuture) { t: UnitTask =>
        executor.kill(task) shouldBe None
        executor.contains(t) shouldBe false
      }
    }
  }

  // NB: needs the LocalTaskExecutor to execute the onComplete method prior to scheduling another task!
  /*
  it should "not run tasks concurrently with more Cores than are defined in the system" in {
    import scala.concurrent.blocking

    val systemCores       = 4
    var allocatedCores    = 0
    var maxAllocatedCores = 0
    val lock              = new Object
    var nameIndex         = 0

    // A task that would like 1-8 cores each
    class HungryTask extends ProcessTask {
      var coresGiven = 0
      override def args: Seq[Any] = "exit" :: "0" :: Nil

      name = lock.synchronized { yieldAndThen(s"$name-$nameIndex")(nameIndex + 1) }

      info(this)

      override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
        (systemCores*2 to 1 by -1)
          .map{ c => ResourceSet(Cores(c), Memory("1g")) }
          .find(rs => availableResources.subset(rs).isDefined)
      }

      override def applyResources(resources: ResourceSet): Unit = {
        coresGiven = resources.cores.toInt
        blocking {
          lock.synchronized {
            allocatedCores += coresGiven
            maxAllocatedCores = Math.max(maxAllocatedCores, allocatedCores)
          }
        }
      }

      override def onComplete(exitCode: Int): Boolean = {
        blocking {
          lock.synchronized {
            allocatedCores -= coresGiven
          }
        }
        super.onComplete(exitCode)
      }
    }

    val resources = SystemResources(systemCores, Resource.parseSizeToBytes("8g").toLong, 0.toLong)

    val executor = new LocalTaskExecutor(systemResources=resources, scriptDirectory=Some(scriptDirectory), logDirectory=Some(logDirectory))

    Seq(new HungryTask, new HungryTask, new HungryTask).map { task =>
      executor.execute(task.asInstanceOf[UnitTask])
    } foreach { future =>
      whenReady(future.flatMap(identity)) { task: UnitTask =>
        executor.contains(task) shouldBe false
        task.onComplete(0)
        // TODO: check the amount of resources it ran with
      }
    }

    maxAllocatedCores should be <= systemCores
  }
  */

  // TODO: test that both tasks that fail or succeed are removed
}
