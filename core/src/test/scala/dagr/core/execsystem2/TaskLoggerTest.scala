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
import dagr.core.execsystem2.local.LocalTaskExecutor
import dagr.core.tasksystem.NoOpInJvmTask

class TaskLoggerTest extends FutureUnitSpec {

  private def toLoggerOutputStream(): ByteArrayOutputStream = {
    val loggerOutputStream = new ByteArrayOutputStream()
    val loggerPrintStream = new PrintStream(loggerOutputStream)
    Logger.out = loggerPrintStream
    loggerOutputStream
  }

  "TopLikeStatusReporter" should "have predicates for the task statuses" in {
    val task = new NoOpInJvmTask("name")
    val taskExecutor = new LocalTaskExecutor()
    val reporter = new TopLikeStatusReporter(
      systemResources = taskExecutor.resources,
      loggerOut       = Some(toLoggerOutputStream()),
      print           = s => System.out.print(s)
    )

    new TaskInfo(task, Pending)

    // running
    task.taskInfo.status = Running
    reporter.running(task) shouldBe true
    task.taskInfo.status = FailedExecution
    reporter.running(task) shouldBe false

    // queued
    Seq(Queued, Submitted).foreach { status =>
      task.taskInfo.status = status
      reporter.queued(task) shouldBe true
    }
    task.taskInfo.status = FailedExecution
    reporter.running(task) shouldBe false

    // failed + completed
    Seq(FailedToBuild, FailedSubmission, FailedExecution, FailedOnComplete, FailedUnknown).foreach { status =>
      task.taskInfo.status = status
      reporter.failed(task) shouldBe true
      reporter.completed(task) shouldBe true
    }
    task.taskInfo.status = Pending
    reporter.failed(task) shouldBe false
    reporter.completed(task) shouldBe false

    // succeeded
    task.taskInfo.status = SucceededExecution
    reporter.succeeded(task) shouldBe true
    task.taskInfo.status = Pending
    reporter.succeeded(task) shouldBe false

    // completed
    Seq(SucceededExecution, ManuallySucceeded).foreach { status =>
      task.taskInfo.status = status
      reporter.completed(task) shouldBe true
    }
    task.taskInfo.status = Pending
    reporter.completed(task) shouldBe false

    // pending
    task.taskInfo.status = Pending
    reporter.pending(task) shouldBe true
    task.taskInfo.status = SucceededExecution
    reporter.pending(task) shouldBe false
  }
}
