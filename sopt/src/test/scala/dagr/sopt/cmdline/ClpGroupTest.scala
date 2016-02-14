/*
 * The MIT License
 *
 * Copyright (c) 2015-2016 Fulcrum Genomics LLC
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
package dagr.sopt.cmdline

import dagr.commons.util.UnitSpec

class ClpsGroupOne extends ClpGroup {
  val name: String = "AAAAA"
  val description: String = "Various pipeline programs."
}

class ClpsGroupTwo extends ClpGroup {
  val name: String = "BBBBB"
  val description: String = "Various pipeline programs."
}
class ClpGroupTest extends UnitSpec {

  "ClpGroup" should "sort groups by alphabetical ordering of name" in {
    val a = new ClpsGroupOne
    val b = new ClpsGroupTwo
    a.compareTo(b) should be < 0
    a.compareTo(a) shouldBe 0
    b.compareTo(a) should be > 0
  }
}
