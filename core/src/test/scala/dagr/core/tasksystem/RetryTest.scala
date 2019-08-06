/*
 * The MIT License
 *
 * Copyright (c) 2016 Fulcrum Genomics LLC
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

import java.nio.file.{Files, Paths}

import com.fulcrumgenomics.commons.io.Io
import dagr.core.UnitSpec
import com.fulcrumgenomics.commons.CommonsDef._
import dagr.core.execsystem._
import org.scalatest.OptionValues

/** Tests for MemoryRetry */
class RetryTest extends UnitSpec with OptionValues {

  private val systemResources = SystemResources(Cores(1.0), Memory("4G"), Memory("4G"))
  private def taskInfo(task: Task) = {
    task match {
      case f: FixedResources => new TaskExecutionInfo(task, 1, TaskStatus.UNKNOWN, null, null, None, resources=f.resources)
      case _ => new TaskExecutionInfo(task, 1, TaskStatus.UNKNOWN, null, null, None)
    }
  }

  "LinearMemoryRetry" should "increase memory linearly upon each retry" in {
    val from = "1G"
    val to   = "2G"
    val by   = "512M"

    val task = new Task with LinearMemoryRetry {
      requires(1, from)
      override def applyResources(resources : ResourceSet): Unit = ()
      def getTasks: Iterable[_ <: Task] = List.empty
      override def toMemory: Memory = Memory(to)
      override def byMemory: Memory = Memory(by)
    }
    Resource.parseBytesToSize(task.asInstanceOf[FixedResources].resources.memory.value) shouldBe "1024m"

    // from 1G to 1.5G
    task.retry(systemResources, taskInfo(task)) shouldBe true
    Resource.parseBytesToSize(task.asInstanceOf[FixedResources].resources.memory.value) shouldBe "1536m"

    // from 1.5G to 2G
    task.retry(systemResources, taskInfo(task)) shouldBe true
    Resource.parseBytesToSize(task.asInstanceOf[FixedResources].resources.memory.value) shouldBe "2048m"

    // 2G is the limit
    task.retry(systemResources, taskInfo(task)) shouldBe false
  }

  "MemoryDoublingRetry" should "should double the memory upon each retry" in {
    val from = "1G"

    val task = new Task with MemoryDoublingRetry {
      requires(1, from)
      override def applyResources(resources : ResourceSet): Unit = ()
      def getTasks: Iterable[_ <: Task] = List.empty
    }
    Resource.parseBytesToSize(task.asInstanceOf[FixedResources].resources.memory.value) shouldBe "1024m"

    // from 1G to 2G
    task.retry(systemResources, taskInfo(task)) shouldBe true
    Resource.parseBytesToSize(task.asInstanceOf[FixedResources].resources.memory.value) shouldBe "2048m"

    // from 2G to 4G
    task.retry(systemResources, taskInfo(task)) shouldBe true
    Resource.parseBytesToSize(task.asInstanceOf[FixedResources].resources.memory.value) shouldBe "4096m"

    // 4G is the limit
    task.retry(systemResources, taskInfo(task)) shouldBe false
  }

  "MultipleRetry" should "retry a fixed number of times" in {
    val task = new Task with MultipleRetry {
      def maxNumIterations: Int = 3
      def getTasks: Iterable[_ <: Task] = List.empty
    }
    val info = taskInfo(task)
    task.retry(systemResources, info) shouldBe true
    info.attemptIndex += 1
    task.retry(systemResources, info) shouldBe true
    info.attemptIndex += 1
    task.retry(systemResources, info) shouldBe true
    info.attemptIndex += 1
    task.retry(systemResources, info) shouldBe false
  }

  "JvmRanOutOfMemory.ranOutOfMemory" should "return true when it finds OOM tokens in the log file" in {
    val task = new ShellCommand("exit", "0") with JvmRanOutOfMemory with MemoryDoublingRetry

    task.outOfMemoryTokens.foreach {
      token =>
        val logFile = Files.createTempFile("log.", ".txt")
        logFile.toFile.deleteOnExit()

        Io.writeLines(logFile, Seq(
          "This is an inoffensive line in a log file.",
          s"It seems your program: $token (that's unfortunate).",
          "Oh well."
        ))


        val info = task match {
          case f: FixedResources => new TaskExecutionInfo(task, 1, TaskStatus.UNKNOWN, null, logFile=logFile, None, resources=f.resources)
          case _ => unreachable()
        }
        task.ranOutOfMemory(info) shouldBe true
    }
  }

  it should "return false when the log file does not exist" in {
    val task = new ShellCommand("exit", "0") with JvmRanOutOfMemory with MemoryDoublingRetry
    val info = task match {
      case f: FixedResources => new TaskExecutionInfo(task, 1, TaskStatus.UNKNOWN, null, logFile=Paths.get("DNE"), None, resources=f.resources)
      case _ => unreachable()
    }
    task.ranOutOfMemory(info) shouldBe false
  }
}
