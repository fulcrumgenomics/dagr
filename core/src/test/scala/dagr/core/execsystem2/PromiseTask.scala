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

import dagr.api.models.ResourceSet
import dagr.core.tasksystem.SimpleInJvmTask

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Promise, TimeoutException}

object PromiseTask {
  def apply(duration: Duration = Duration.Inf,
            resourceSet: ResourceSet = ResourceSet.Inf)
           (implicit ex: ExecutionContext): PromiseTask = {
    new PromiseTask(duration=duration, resourceSet=resourceSet)
  }
}

/** A task that does not complete until the promise is completed. */
class PromiseTask(duration: Duration = Duration.Inf,
                  resourceSet: ResourceSet = ResourceSet.Inf)
                 (implicit ex: ExecutionContext) extends SimpleInJvmTask {
  val promise: Promise[Int] = Promise[Int]()
  requires(resourceSet)
  override def run() = try {
    Await.result(promise.future, duration)
  } catch {
    case e: TimeoutException => this.promise.success(-1)
  }
}

