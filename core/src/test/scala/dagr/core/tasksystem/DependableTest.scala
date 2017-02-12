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

  class TrivialTask extends UnitTask with FixedResources { override def toString: String = name }
  val A = new TrivialTask().withName("A")
  val B = new TrivialTask().withName("B")
  val C = new TrivialTask().withName("C")
  val X = new TrivialTask().withName("X")
  val Y = new TrivialTask().withName("Y")
  val Z = new TrivialTask().withName("Z")

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

  it should "construct chains of tasks" in {
    (X ==> Y ==> Z) shouldBe DependencyChain(DependencyChain(X, Y), Z)
    (Some(X) ==> Y ==> Some(Z)) shouldBe DependencyChain(DependencyChain(X, Y), Z)
  }

  it should "support adding dependencies onto existing chains" in {
    val abc = A ==> B ==> C
    val xabcy = X ==> abc ==> Y
    xabcy shouldBe DependencyChain(DependencyChain(X,DependencyChain(DependencyChain(A,B),C)),Y)
  }

  "Dependable.::" should "accept options and drop Nones when amassing a group" in {
    (None :: X :: None) shouldBe X
    (None :: Some(X) :: None) shouldBe X
    (None :: X) shouldBe X
    (X :: None) shouldBe X
    (X :: None :: None :: Some(Y) :: None) shouldBe DependencyGroup(X, Y)
  }

  it should "build groups of tasks" in {
    (X :: Y :: Z).allTasks should contain theSameElementsAs Seq(X, Y, Z)
    (None :: Some(X) :: None :: Y :: Some(Z)).allTasks should contain theSameElementsAs Seq(X, Y, Z)
  }

  "EmptyDependable.toTasks" should "always return no tasks" in {
    (None :: None :: None).allTasks shouldBe empty
  }
}
