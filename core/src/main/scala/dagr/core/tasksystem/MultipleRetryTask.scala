/*
 * The MIT License
 *
 * Copyright (c) 2015 Fulcrum Genomics LLC
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

import dagr.core.execsystem.TaskExecutionInfo
import dagr.core.util.LazyLogging

/** Simple task to retry a given number of times */
trait MultipleRetryTask extends Task with LazyLogging {

  /**
   *
   * @return the maximum number iterations to retry.
   */
  def maxNumIterations: Int

  /** Retry the given task.
    *
    * @param taskInfo the task execution information for the task to be retried.
    * @param failedOnComplete true the task failed while running its onComplete method, otherwise failed while running its command
    * @return the task to be retried, None if it is not to be retried.
    */
  override def retry(taskInfo: TaskExecutionInfo, failedOnComplete: Boolean): Option[Task] = {
    logger.debug("maxNumIterations=" + maxNumIterations + " taskInfo.attemptIndex=" + taskInfo.attemptIndex)
    if (maxNumIterations <= taskInfo.attemptIndex) None
    else Some(taskInfo.task)
  }
}
