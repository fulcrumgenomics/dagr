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
package dagr

import dagr.DagrDef._
import dagr.core.util.UnitSpec

import scala.collection.mutable.ListBuffer

/**
  * Tests for DagrDef
  */
class DagrDefTest extends UnitSpec {
  "DagrDef.unreachable" should "always throw an exception" in {
    an[RuntimeException] shouldBe thrownBy { unreachable() }
    an[RuntimeException] shouldBe thrownBy { unreachable("booya!") }
    an[RuntimeException] shouldBe thrownBy { None orElse unreachable("booya!") }
    an[RuntimeException] shouldBe thrownBy { None getOrElse unreachable("booya!") }
    Option("foo") getOrElse unreachable("truly unreachable") shouldBe "foo"
  }

  "DagrDef.yieldAndThen" should "capture the value before running the code block" in {
    val xs = ListBuffer[String]()
    def doIt() = yieldAndThen(xs.length) { xs += "foo" }

    doIt() shouldBe 0
    doIt() shouldBe 1
    doIt() shouldBe 2
    xs shouldBe List("foo", "foo", "foo")
  }
}
