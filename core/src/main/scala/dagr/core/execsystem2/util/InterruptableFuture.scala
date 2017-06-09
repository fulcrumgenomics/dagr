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

package dagr.core.execsystem2.util

import java.util.concurrent.CancellationException

import com.fulcrumgenomics.commons.CommonsDef.yieldAndThen

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.duration.Duration

object InterruptableFuture {
  /** Creates an interruptable future given the method that produces a result of type [[T]] */
  def apply[T](work: => T)(implicit ex: ExecutionContext): InterruptableFuture[T] = {
    new InterruptableFuture[T]({ future => if (future.isCompleted) throw new InterruptedException else work })
  }

  /**
    * Do not use the future that you pass to this method.  Instead, rely on the returned [[InterruptableFuture.future]].
    */
  def apply[T](future: Future[T])(implicit ex: ExecutionContext): InterruptableFuture[T] = {
    val work = () => Await.result(future, Duration.Inf)
    InterruptableFuture(work=work())
  }

  /** A little implicit to convert a future to an interruptable future. */
  implicit class Interruptable[T](private val future: Future[T])(implicit ex: ExecutionContext) {
    def interruptable() : InterruptableFuture[T] = InterruptableFuture(future)
  }

  /** Converts a [[Future]] to an [[InterruptableFuture]] */
  implicit def toInterruptable[T](future: InterruptableFuture[T])(implicit ex: ExecutionContext): Future[T] =
    future.future
}

/**
  * A wrapper for a future such that the future can be interrupted.  The provided future should not be used, but
  * instead, the future returned by the [[future()]] method.  The [[interrupt()]] method can be used to interrupt the
  * underlying future.
  *
  * Adapted from https://gist.github.com/viktorklang/5409467
  */
class InterruptableFuture[T](fun: Future[T] => T)(implicit ex: ExecutionContext) {
  // True if the underlying future has been interrupted.
  private var _interrupted: Boolean = false

  // Some result if the underlying future completed with a result.
  private var result: Option[T] = None

  // The current thread to interrupt.
  private var currentThread: Option[Thread] = None

  // A lock to synchronize interruption and completion
  private val lock = new Object

  // A promise used to return a new future on which callers can await or depend
  private val promise = Promise[T]()

  // The future we promised to the caller.
  private def promisedFuture = promise.future

  /** Sets the interrupt flag to true, and throws a [[CancellationException]]. */
  private def interruptWithException(): T = {
    this._interrupted = true
    throw new CancellationException
  }

  /** The future on which the caller should depend. */
  def future: Future[T] = this.promise.future

  /** Interrupts the underlying future.  Returns the result if the future has already completed, None otherwise. */
  def interrupt(): Option[T] = this.lock.synchronized {
    // only interrupt if the previous thread was null
    this.currentThread.foreach(_.interrupt())
    this.currentThread = None
    this.promise.tryFailure(new CancellationException)
    this._interrupted = true
    this.result
  }

  /** Returns true if the underlying future as interrupted, false otherwise. */
  def interrupted: Boolean = this.result.isEmpty && this._interrupted

  // Complete the promise with the given future.
  this.promise tryCompleteWith Future {
    // Get the current thread.  May be None if the promise completed while waiting for the lock.
    val thread = this.lock.synchronized {
      if (this.promisedFuture.isCompleted) {
        interruptWithException()
      }
      else {
        val t = Thread.currentThread
        yieldAndThen(Some(t))(this.currentThread=Some(t))
      }
    }
    thread match {
      case None            => interruptWithException()
      case Some(t: Thread) =>
        try {
          // execute the future and return the result!
          val r = fun(this.promisedFuture)
          yieldAndThen(r)(this.result=Some(r))
        } catch {
          case _: InterruptedException => interruptWithException()
        } finally {
          // Synchronously set the thread to None and return the original thread
          val originalThread = this.lock.synchronized {
            yieldAndThen(this.currentThread)(this.currentThread=None)
          }
          // It is only interrupted if the current execution thread was set to None
          this._interrupted = !originalThread.contains(t)
        }
    }
  }
}