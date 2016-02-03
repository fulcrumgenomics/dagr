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
package dagr.core.util

import java.util

object ReflectionUtilTest {
  case class IntNoDefault(var v: Int)
  case class TwoParamNoDefault(var v: Int = 2, var w: Int)
  case class IntDefault(var v: Int = 2)
  case class DefaultWithOption(var w: Option[Int] = None)
  case class NoParams()
  case class StringDefault(var s: String = "null")
  case class ComplexDefault(var v: StringDefault = new StringDefault())
  case class ComplexNoDefault(var v: StringDefault)
}

class ReflectionUtilTest extends UnitSpec {
  def newJavaCollectionInstanceTest[T <: java.util.Collection[_]](clazz: Class[T]): Unit = {
    val c: util.Collection[_] = ReflectionUtil.newJavaCollectionInstance(clazz, Seq[AnyRef]("1", "2", "3"))
    c should have size 3
    Seq("1", "2", "3").foreach { v => c should contain(v) }
  }

  "ReflectionHelper.newJavaCollectionInstance" should "create a java.util.Collection[_]" in {
    newJavaCollectionInstanceTest(clazz = classOf[java.util.Collection[_]])
  }

  it should "create a java.util.List[_]" in {
    newJavaCollectionInstanceTest(clazz = classOf[java.util.List[_]])
  }

  it should "create a java.util.Set[_]" in {
    newJavaCollectionInstanceTest(clazz = classOf[java.util.Set[_]])
  }

  it should "create a java.util.SortedSet[_]" in {
    newJavaCollectionInstanceTest(clazz = classOf[java.util.SortedSet[_]])
  }

  it should "create a java.util.NavigableSet[_]" in {
    newJavaCollectionInstanceTest(clazz = classOf[java.util.NavigableSet[_]])
  }

  "ReflectionUtil.newScalaCollection" should "instantiate scala.collection.mutable.Seq[Int]" in {
    ReflectionUtil.newScalaCollection(classOf[scala.collection.mutable.Seq[Int]], Seq(1,2,3)) should be (List(1,2,3))
    ReflectionUtil.newScalaCollection(classOf[scala.collection.mutable.Seq[Int]], Nil) should have size 0
  }

  it should "instantiate scala.collection.mutable.Seq[_]" in {
    ReflectionUtil.newScalaCollection(classOf[scala.collection.mutable.Seq[_]], List(1,2,3)) should be (List(1,2,3))
    ReflectionUtil.newScalaCollection(classOf[scala.collection.mutable.Seq[_]], Nil) should have size 0
  }

  it should "instantiate scala.collection.immutable.Seq[Int]" in {
    ReflectionUtil.newScalaCollection(classOf[scala.collection.immutable.Seq[Int]], List(1,2,3)) should be (List(1,2,3))
    ReflectionUtil.newScalaCollection(classOf[scala.collection.immutable.Seq[Int]], Nil) should have size 0
  }

  it should "instantiate scala.collection.immutable.Seq[_]" in {
    ReflectionUtil.newScalaCollection(classOf[scala.collection.immutable.Seq[_]], List(1,2,3)) should be (List(1,2,3))
    ReflectionUtil.newScalaCollection(classOf[scala.collection.immutable.Seq[_]], Nil) should have size 0
  }

  it should "instantiate scala.collection.Seq[Int]" in {
    ReflectionUtil.newScalaCollection(classOf[scala.collection.Seq[Int]], List(1,2,3)) should be (List(1,2,3))
    ReflectionUtil.newScalaCollection(classOf[scala.collection.Seq[Int]], Nil) should have size 0
  }

  it should "instantiate scala.collection.Seq[_]" in {
    ReflectionUtil.newScalaCollection(classOf[scala.collection.Seq[_]], List(1,2,3)) should be (List(1,2,3))
    ReflectionUtil.newScalaCollection(classOf[scala.collection.Seq[_]], Nil) should have size 0
  }

  "ReflectionUtil.ifPrimitiveThenWrapper" should "get type for java primitive types" in {
    ReflectionUtil.ifPrimitiveThenWrapper(java.lang.Boolean.TYPE) shouldBe classOf[java.lang.Boolean]
    ReflectionUtil.ifPrimitiveThenWrapper(java.lang.Integer.TYPE) shouldBe classOf[java.lang.Integer]
    ReflectionUtil.ifPrimitiveThenWrapper(classOf[LogLevel]) shouldBe classOf[LogLevel]
  }

}
