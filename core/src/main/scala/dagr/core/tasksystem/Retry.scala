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

import java.nio.file.Files

import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.execsystem._
import com.fulcrumgenomics.commons.io.Io

/** A trait to facilitate retry a task when it has failed. */
trait Retry {
  /** This method will be invoked to allow the task to perform any self-modification before retrying.
    *
    * @param resources the maximum system resources available to the task
    * @param taskInfo the task execution information for the task to be retried.
    * @return true if the task is to be retried, false otherwise.
    */
  def retry(resources: SystemResources, taskInfo: TaskExecutionInfo): Boolean = false
}

/**
  * A trait to facilitate retrying a task with more memory.
  */
trait MemoryRetry extends Retry with FixedResources {
  /** Given the current memory, returns the next memory to retry this task with, or None, if the task
    * should not be retried.
    */
  protected def nextMemory(currentMemory: Memory): Option[Memory]

  /** True if we should use the system memory as the maximum memory, false to use the heap size. */
  protected def useSystemMemory: Boolean = true

  /** Determines if this task ran out of memory, and if so, it will get the next memory value with which to be retried. */
  protected def ranOutOfMemory(taskInfo: TaskExecutionInfo): Boolean = true

  /** Tries to get the next memory value with which to run task.  It will enforce tha the task will not require more
    * memory than in the system.
    * @param systemResources the system resources under which this task should run.
    * @param taskInfo the task execution information for the task to be retried.
    * @return true if the task is to be retried, false otherwise.
    */
  override final def retry(systemResources: SystemResources, taskInfo: TaskExecutionInfo): Boolean = {
    if (taskInfo.resources.memory != this.resources.memory) throw new IllegalStateException("Scheduled memory does not equal current memory")
    if (ranOutOfMemory(taskInfo)) {
      val maximumMemory = if (useSystemMemory) systemResources.systemMemory else systemResources.jvmMemory
      nextMemory(this.resources.memory)
        .filter { _ <= maximumMemory }
        .map { memory => this.requires(this.resources.cores, memory) }
        .isDefined
    }
    else {
      false
    }
  }
}

/**
  * A trait for all tasks that wish to linearly increase their memory upon each retry.
  *
  *  By default retries until the maximum system memory is reached, and increases by the initial amount of memory required.
  */
trait LinearMemoryRetry extends MemoryRetry {
  private lazy val initialMemory: Memory = this.resources.memory

  /** The maximum amount of memory with which to retry. */
  def toMemory: Memory = Memory.infinite

  /** The amount of memory to increase by on each retry. */
  def byMemory: Memory = initialMemory

  override protected def nextMemory(currentMemory: Memory): Option[Memory] = {
    val nextMemory: Memory = currentMemory + byMemory
    if (nextMemory <= toMemory) Some(nextMemory)
    else None
  }
}

/**
  * A trait for all tasks that wish to double their memory upon each retry.
  */
trait MemoryDoublingRetry extends MemoryRetry {
  override protected def nextMemory(currentMemory: Memory): Option[Memory] = {
    Some(currentMemory + currentMemory)
  }
}

/** Simple trait to retry a given number of times */
trait MultipleRetry extends Retry with LazyLogging {
  /** returns the maximum number iterations to retry. */
  def maxNumIterations: Int

  override def retry(resources: SystemResources, taskInfo: TaskExecutionInfo): Boolean = {
    logger.debug("maxNumIterations=" + maxNumIterations + " taskInfo.attemptIndex=" + taskInfo.attemptIndex)
    taskInfo.attemptIndex <= maxNumIterations
  }
}

/** Determines if a the task failed because it ran out of memory by looking for various strings in the log file. */
trait JvmRanOutOfMemory extends MemoryRetry {
  /** A list of tokens that are looked for in the log file of a process by the default [[ranOutOfMemory]]. */
  def outOfMemoryTokens: List[String] =  List ("OutOfMemory", "you did not provide enough memory to run this program")

  override protected[core] def ranOutOfMemory(taskInfo: TaskExecutionInfo): Boolean = {
    Files.exists(taskInfo.logFile) &&
      Io.readLines(taskInfo.logFile).exists(line => outOfMemoryTokens.exists(token => line.contains(token)))
  }
}
