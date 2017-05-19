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

package dagr.core

import org.scalatest.{Matchers, OptionValues, AsyncFlatSpec => ScalaTestAsyncFlatSpec}
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}
import java.util.concurrent.Executors

object FutureUnitSpec {
  val nThreads: Int = 1000 // not to be used for cpu bound work
  val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(nThreads))
}

class FutureUnitSpec extends UnitSpec with OptionValues with ScalaFutures {
  implicit val ec: ExecutionContext = FutureUnitSpec.ec

  implicit override val patienceConfig = FutureUnitSpec.this.PatienceConfig(timeout = Span(10, Seconds), interval = Span(20, Millis))
}
