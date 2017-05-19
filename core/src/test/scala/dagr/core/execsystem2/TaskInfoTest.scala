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

import java.time.Instant

import dagr.core.UnitSpec
import dagr.core.tasksystem.{NoOpInJvmTask, Task}
import org.scalatest.OptionValues

class TaskInfoTest extends UnitSpec with OptionValues {
  private def task: Task = new NoOpInJvmTask("name")
  
  "TaskInfo.submissionDate" should "be the latest instant of Pending" in {
    val info = new TaskInfo(task=task, initStatus=Queued)
    val instant = Instant.now()
    info.update(Pending, instant)
    info.submissionDate.value shouldBe instant
  }

  "TaskInfo.startDate" should "be the latest instant of Running" in {
    val info = new TaskInfo(task=task, initStatus=Queued)
    val instant = Instant.now()
    info.update(Running, instant)
    info.startDate.value shouldBe instant
  }

  "TaskInfo.endDate" should "be the latest instant of Completed" in {
    val info = new TaskInfo(task=task, initStatus=Queued)
    val instant = Instant.now()
    info.update(SucceededExecution, instant)
    info.endDate.value shouldBe instant
  }
}
