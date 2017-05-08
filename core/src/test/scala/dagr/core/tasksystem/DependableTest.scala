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

import dagr.core.UnitSpec

class DependableTest extends UnitSpec {
  import EmptyDependable.optionDependableToDependable // import the implicit

  case class TrivialTask(x: String) extends UnitTask with FixedResources { override def toString: String = x }
  val A = TrivialTask("A")
  val B = TrivialTask("B")
  val C = TrivialTask("C")
  val X = TrivialTask("X")
  val Y = TrivialTask("Y")
  val Z = TrivialTask("Z")

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

  "EmptyDependable" should "always return Nil from it's *tasks methods" in {
    (None ==> None).headTasks shouldBe Nil
    (None ==> None).tailTasks shouldBe Nil
    (None ==> None).allTasks  shouldBe Nil
  }

  "Dependable.!=>" should "remove existing dependencies" in {
    val Seq(a, b, c, d, e, f) = "abcdef".map(ch => TrivialTask(ch.toString))
    (a :: b :: c) ==> (d :: e :: f)
    a !=> d
    (b :: c) !=> (d :: e)

    a.tasksDependingOnThisTask should contain theSameElementsAs Seq(e, f)
    b.tasksDependingOnThisTask should contain theSameElementsAs Seq(f)
    c.tasksDependingOnThisTask should contain theSameElementsAs Seq(f)
  }

  "Dependable" should "wire together a whole mess of tasks" in {
    val Seq(a, b, c, d, e, f, g, h, i, j, k) = "abcdefghijk".map(ch => TrivialTask(ch.toString))
    val bcd   = b ==> c ==> d
    val abcde = a ==> bcd ==> Some(e)
    val fgh   = None :: Some(f) :: g :: Some(h) :: None
    val ijk   = i ==> (j :: k)

    abcde ==> fgh ==> ijk // should be a -> b -> c -> d -> e -> (f :: g :: h) -> i -> (j :: k)

    a.tasksDependingOnThisTask should contain theSameElementsAs Seq(b)
    b.tasksDependingOnThisTask should contain theSameElementsAs Seq(c)
    c.tasksDependingOnThisTask should contain theSameElementsAs Seq(d)
    d.tasksDependingOnThisTask should contain theSameElementsAs Seq(e)
    e.tasksDependingOnThisTask should contain theSameElementsAs Seq(f, g, h)
    f.tasksDependingOnThisTask should contain theSameElementsAs Seq(i)
    g.tasksDependingOnThisTask should contain theSameElementsAs Seq(i)
    h.tasksDependingOnThisTask should contain theSameElementsAs Seq(i)
    i.tasksDependingOnThisTask should contain theSameElementsAs Seq(j, k)
    j.tasksDependingOnThisTask should contain theSameElementsAs Seq()
    k.tasksDependingOnThisTask should contain theSameElementsAs Seq()

    a.tasksDependedOn should contain theSameElementsAs Seq()
    b.tasksDependedOn should contain theSameElementsAs Seq(a)
    c.tasksDependedOn should contain theSameElementsAs Seq(b)
    d.tasksDependedOn should contain theSameElementsAs Seq(c)
    e.tasksDependedOn should contain theSameElementsAs Seq(d)
    f.tasksDependedOn should contain theSameElementsAs Seq(e)
    g.tasksDependedOn should contain theSameElementsAs Seq(e)
    h.tasksDependedOn should contain theSameElementsAs Seq(e)
    i.tasksDependedOn should contain theSameElementsAs Seq(f, g, h)
    j.tasksDependedOn should contain theSameElementsAs Seq(i)
    k.tasksDependedOn should contain theSameElementsAs Seq(i)
  }

  "Pipeline.root" should "return the same things as Pipeline from the *tasks methods" in {
    val pipeline = new Pipeline() {
      override def build() = root ==> (A :: B :: C) ==> (X :: Y :: Z)
    }

    pipeline.root.headTasks shouldBe pipeline.headTasks
    pipeline.root.tailTasks shouldBe pipeline.tailTasks
    pipeline.root.allTasks  shouldBe pipeline.allTasks
  }
}
