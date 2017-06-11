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

import dagr.core.FutureUnitSpec
import dagr.core.execsystem2.util.InterruptableFuture.Interruptable

import scala.concurrent.Future
import scala.util.Success


class InterruptableFutureTest extends FutureUnitSpec {


  // NB: cannot ensure that the code block executes after interrupt() to test this
  /*
  "InterruptableFuture.apply" should "not invoke the work block of code if it was interrupted before it was invoked" in {
  }
  */

  // NB: cannot ensure that the code block executes after interrupt() to test this
  /*
  "InterruptableFuture" should "ignore the result of the future an execute anyway" in {
  }
  */

  "InterruptableFuture" should "support being interrupted before the work body is completed" in {
    val future: InterruptableFuture[Int] = new InterruptableFuture[Int](fun = _ => { Thread.sleep(1000); 2 })

    future.interrupt() shouldBe None

    future.future

    whenReady(future.future.failed) { result: Throwable =>
      result shouldBe a [CancellationException]
      future.interrupted shouldBe true
    }
  }

  it should "complete successfully if it is never interrupted" in {
    val future: InterruptableFuture[Int] = new InterruptableFuture[Int](fun = _ => 2)
    whenReady(future.future) { _ shouldBe 2 }
  }

  it should "support returning a Future[T]" in {
    val future: InterruptableFuture[Future[Int]] = new InterruptableFuture[Future[Int]](fun = _ => Future {
      2
    })
    whenReady(future.future) { result: Future[Int] =>
      whenReady(result) { _ shouldBe 2 }
    }
  }

  it should "support interrupting before constructing a future type" in {
    var internalFuture: Option[Future[Int]] = None
    val fun: () => Future[Int] = () => {
      Thread.sleep(100000)
      val future = Future { 2 }
      internalFuture = Some(future)
      future
    }

    val future: InterruptableFuture[Future[Int]] = InterruptableFuture[Future[Int]](work=fun())

    future.interrupt() shouldBe None

    whenReady(future.future.failed) { result: Throwable =>
      result shouldBe a [CancellationException]
      future.interrupted shouldBe true
    }

    internalFuture shouldBe None
  }

  it should  "support interrupting when given a future to wrap" in {
    val futureToWrap: Future[Int] = Future { Thread.sleep(100000); 2 }
    val interruptableFuture: InterruptableFuture[Int] = InterruptableFuture(futureToWrap)
    val futureToUse: Future[Int] = interruptableFuture.future

    interruptableFuture.interrupt() shouldBe None

    whenReady(interruptableFuture.future.failed) { result: Throwable =>
      result shouldBe a [CancellationException]
      interruptableFuture.interrupted shouldBe true
    }

    futureToWrap.isCompleted shouldBe false

    whenReady(futureToUse.failed) { result: Throwable =>
      result shouldBe a [CancellationException]
    }
  }

  "InterruptableFuture.apply" should "complete successfully if it is never interrupted" in {
    val work: () => Int = () => 2
    val future = InterruptableFuture[Int] { work() }
    whenReady(future.future) { _ shouldBe 2 }
  }

  "InterruptableFuture.Interruptable" should "convert a Future[T] to an InterruptableFuture[T]" in {
    val interruptableFuture = Future { Thread.sleep(100000); 2 } interruptable()

    interruptableFuture.interrupt() shouldBe None

    whenReady(interruptableFuture.future.failed) { result: Throwable =>
      result shouldBe a [CancellationException]
      interruptableFuture.interrupted shouldBe true
    }

    whenReady(interruptableFuture.future.failed) { result: Throwable =>
      result shouldBe a [CancellationException]
    }
  }

  "InterruptableFuture.interrupted" should "be false if the work body completes before being interrupted" in {
    val future: InterruptableFuture[Int] = new InterruptableFuture[Int](fun = _ => 2 )

    whenReady(future.future) { result: Int =>
      future.interrupt().value shouldBe 2
      future.interrupted shouldBe false
     result shouldBe 2
    }
  }

  it should "true even if it is interrupted multiple times" in {
    val future: InterruptableFuture[Int] = new InterruptableFuture[Int](fun = _ => 2 )

    whenReady(future.future) { result =>
      Range(1, 10).foreach { _ =>
        future.interrupt().value shouldBe 2
        future.interrupted shouldBe false

        future.future.isCompleted shouldBe true
        future.future.value.isDefined shouldBe true
        future.future.value.value shouldBe Success(2)
      }
      result shouldBe 2
    }
  }
}
