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
 */

package dagr.core.tasksystem

import dagr.commons.util.UnitSpec

class DependableTest extends UnitSpec {
  import EmptyDependable.optionDependableToDependable // import the implicit

  val X = new ShellCommand("echo", "hello", "world")
  val Y = new ShellCommand("echo", "hello", "world")
  val Z = new ShellCommand("echo", "hello", "world")

  "Dependable.==>" should "return the real task when invoked on a task and a None" in {
    (X ==> None) shouldBe X
    (None ==> X) shouldBe X
    (Some(X) ==> None) shouldBe X
    (None ==> Some(X)) shouldBe X
  }

  it should "collapse things out of the chain when there are Nones in the chain" in {
    (X ==> Y) shouldBe DependencyChain(X, Y)
    (X ==> None ==> None ==> Some(Y)) shouldBe DependencyChain(X, Y)
    (None ==> Some(X) ==> None ==> Some(Y)) shouldBe DependencyChain(X, Y)
  }

  "Dependable.::" should "accept options and drop Nones when amassing a group" in {
    (None :: X :: None) shouldBe X
    (None :: Some(X) :: None) shouldBe X
    (None :: X) shouldBe X
    (X :: None) shouldBe X
    (X :: None :: None :: Some(Y) :: None) shouldBe DependencyGroup(X, Y)
  }
}
