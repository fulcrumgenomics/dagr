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

import java.io.{ByteArrayOutputStream, PrintStream}

import com.fulcrumgenomics.commons.util.Logger
import dagr.core.FutureUnitSpec
import dagr.core.execsystem2.TaskStatus._
import dagr.core.execsystem2.local.LocalTaskExecutor
import dagr.core.reporting.TopLikeStatusReporter
import dagr.core.tasksystem.NoOpInJvmTask

class TopLikeStatusReporterTest extends FutureUnitSpec {

  private def toLoggerOutputStream: ByteArrayOutputStream = {
    val loggerOutputStream = new ByteArrayOutputStream()
    val loggerPrintStream = new PrintStream(loggerOutputStream)
    Logger.out = loggerPrintStream
    loggerOutputStream
  }

  "TopLikeStatusReporter" should "have predicates for the task statuses" in {
    val task = new NoOpInJvmTask("name")
    val taskExecutor = new LocalTaskExecutor()
    val executor = Executor(taskExecutor)
    val reporter = new TopLikeStatusReporter(
      executor        = executor,
      systemResources = taskExecutor.resources,
      loggerOut       = Some(toLoggerOutputStream),
      print           = s => Unit
    )

    executor.withReporter(reporter)

    new TaskInfo(task, Pending)

    // running
    task.taskInfo.status = Running
    executor.running(task) shouldBe true
    task.taskInfo.status = FailedExecution
    executor.running(task) shouldBe false

    // queued
    Seq(Queued, Submitted).foreach { status =>
      task.taskInfo.status = status
      executor.queued(task) shouldBe true
    }
    task.taskInfo.status = FailedExecution
    executor.running(task) shouldBe false

    // failed + completed
    Seq(FailedToBuild, FailedSubmission, FailedExecution, FailedOnComplete, FailedUnknown).foreach { status =>
      task.taskInfo.status = status
      executor.failed(task) shouldBe true
      executor.completed(task) shouldBe true
    }
    task.taskInfo.status = Pending
    executor.failed(task) shouldBe false
    executor.completed(task) shouldBe false

    // succeeded
    task.taskInfo.status = SucceededExecution
    executor.succeeded(task) shouldBe true
    task.taskInfo.status = Pending
    executor.succeeded(task) shouldBe false

    // completed
    Seq(SucceededExecution, ManuallySucceeded).foreach { status =>
      task.taskInfo.status = status
      executor.completed(task) shouldBe true
    }
    task.taskInfo.status = Pending
    executor.completed(task) shouldBe false

    // pending
    task.taskInfo.status = Pending
    executor.pending(task) shouldBe true
    task.taskInfo.status = SucceededExecution
    executor.pending(task) shouldBe false
  }
}
