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
import java.util.concurrent.TimeUnit

import scala.annotation.{ClassfileAnnotation, StaticAnnotation}
import scala.reflect.runtime.{universe => ru}

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

/** Tests for many of the methods in ReflectionUtil.  More tests in other classes below! */
class ReflectionUtilTest extends UnitSpec {
  val mirror: ru.Mirror = ru.runtimeMirror(getClass.getClassLoader)

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
    ReflectionUtil.newScalaCollection(classOf[scala.collection.mutable.Seq[Int]], Seq(1, 2, 3)) should be(List(1, 2, 3))
    ReflectionUtil.newScalaCollection(classOf[scala.collection.mutable.Seq[Int]], Nil) should have size 0
  }

  it should "instantiate scala.collection.mutable.Seq[_]" in {
    ReflectionUtil.newScalaCollection(classOf[scala.collection.mutable.Seq[_]], List(1, 2, 3)) should be(List(1, 2, 3))
    ReflectionUtil.newScalaCollection(classOf[scala.collection.mutable.Seq[_]], Nil) should have size 0
  }

  it should "instantiate scala.collection.immutable.Seq[Int]" in {
    ReflectionUtil.newScalaCollection(classOf[scala.collection.immutable.Seq[Int]], List(1, 2, 3)) should be(List(1, 2, 3))
    ReflectionUtil.newScalaCollection(classOf[scala.collection.immutable.Seq[Int]], Nil) should have size 0
  }

  it should "instantiate scala.collection.immutable.Seq[_]" in {
    ReflectionUtil.newScalaCollection(classOf[scala.collection.immutable.Seq[_]], List(1, 2, 3)) should be(List(1, 2, 3))
    ReflectionUtil.newScalaCollection(classOf[scala.collection.immutable.Seq[_]], Nil) should have size 0
  }

  it should "instantiate scala.collection.Seq[Int]" in {
    ReflectionUtil.newScalaCollection(classOf[scala.collection.Seq[Int]], List(1, 2, 3)) should be(List(1, 2, 3))
    ReflectionUtil.newScalaCollection(classOf[scala.collection.Seq[Int]], Nil) should have size 0
  }

  it should "instantiate scala.collection.Seq[_]" in {
    ReflectionUtil.newScalaCollection(classOf[scala.collection.Seq[_]], List(1, 2, 3)) should be(List(1, 2, 3))
    ReflectionUtil.newScalaCollection(classOf[scala.collection.Seq[_]], Nil) should have size 0
  }

  "ReflectionUtil.ifPrimitiveThenWrapper" should "get type for java primitive types" in {
    ReflectionUtil.ifPrimitiveThenWrapper(java.lang.Boolean.TYPE) shouldBe classOf[java.lang.Boolean]
    ReflectionUtil.ifPrimitiveThenWrapper(java.lang.Integer.TYPE) shouldBe classOf[java.lang.Integer]
    ReflectionUtil.ifPrimitiveThenWrapper(classOf[LogLevel]) shouldBe classOf[LogLevel]
  }

  {
    "ReflectionUtil" should "identify various sub-classes of Seq[_] as collection fields" in {
      val classes = List(
        classOf[scala.collection.Seq[_]],
        classOf[scala.collection.IndexedSeq[_]],
        classOf[scala.collection.immutable.Seq[_]],
        classOf[scala.collection.immutable.IndexedSeq[_]],
        classOf[scala.collection.mutable.Seq[_]],
        classOf[scala.collection.mutable.IndexedSeq[_]],
        classOf[scala.collection.mutable.ListBuffer[_]],
        classOf[scala.collection.mutable.ListBuffer[Any]],
        classOf[scala.collection.mutable.ListBuffer[Int]]
      )

      classes.foreach { clazz => ReflectionUtil.isSeqClass(clazz) shouldBe true }
    }

    it should "identify various classes that are not sub-classes of Seq[_] not as seq fields" in {
      val classes = List(
        classOf[scala.collection.Map[_, _]],
        classOf[scala.collection.MapLike[_, _, _]],
        classOf[scala.collection.immutable.Map[_, _]],
        classOf[scala.collection.immutable.ListMap[_, _]],
        classOf[scala.collection.mutable.Map[_, _]],
        classOf[scala.collection.mutable.HashMap[_, _]],
        classOf[scala.collection.mutable.HashSet[_]],
        classOf[Integer],
        classOf[java.util.Collection[_]]
      )

      classes.foreach { clazz => ReflectionUtil.isSeqClass(clazz) shouldBe false }
    }

    it should "identify various sub-classes of Seq[_] or java.util.Collection[_] as collection fields" in {
      val classes = List(
        classOf[scala.collection.Seq[_]],
        classOf[scala.collection.IndexedSeq[_]],
        classOf[scala.collection.immutable.Seq[_]],
        classOf[scala.collection.immutable.IndexedSeq[_]],
        classOf[scala.collection.mutable.Seq[_]],
        classOf[scala.collection.mutable.IndexedSeq[_]],
        classOf[scala.collection.mutable.ListBuffer[_]],
        classOf[scala.collection.mutable.ListBuffer[Any]],
        classOf[scala.collection.mutable.ListBuffer[Int]],
        classOf[java.util.Collection[_]]
      )

      classes.foreach { clazz => ReflectionUtil.isCollectionClass(clazz) shouldBe true }
    }

    it should "identify various classes that are not sub-classes of either Seq[_] or java.util.Collection[_] not as collection fields" in {
      val classes = List(
        classOf[scala.collection.Map[_, _]],
        classOf[scala.collection.MapLike[_, _, _]],
        classOf[scala.collection.immutable.Map[_, _]],
        classOf[scala.collection.immutable.ListMap[_, _]],
        classOf[scala.collection.mutable.Map[_, _]],
        classOf[scala.collection.mutable.HashMap[_, _]],
        classOf[Integer]
      )

      classes.foreach { clazz => ReflectionUtil.isCollectionClass(clazz) shouldBe false }
    }
  }

}


/** Annotation that is used in testing the findAnnotation method (has to be outer class). */
case class Ann(s:String = "foo", i:Int = 42, b:Boolean=false) extends ClassfileAnnotation
@Ann class NoValues
@Ann(s="Hello") class Hello
@Ann(s="Hello", i=101, b=true) class Hello101True
@Ann(s="foo" + "bar", i=2+2, b=false || true) class Expressions
@Ann(s=java.util.jar.JarFile.MANIFEST_NAME, i=Integer.MAX_VALUE) class WithConstRefs

/* Purposefully not a case class, to test both case and non-case class annotations. */
class EnumAnn(val e:TimeUnit= TimeUnit.MINUTES) extends ClassfileAnnotation {
  override def equals(that: scala.Any): Boolean = this.e == that.asInstanceOf[EnumAnn].e
}
@EnumAnn class EnumAnnotated
@EnumAnn(e=TimeUnit.DAYS) class EnumAnnotatedWaiting

case class InnerEnumAnn(val state: Thread.State = Thread.State.NEW) extends ClassfileAnnotation
@InnerEnumAnn(state=Thread.State.BLOCKED) class InnerEnumAnnotated

case class ArrayAnn(val ss: Array[String] = Array()) extends ClassfileAnnotation {
  override def equals(that: scala.Any): Boolean = this.ss.toSeq == that.asInstanceOf[ArrayAnn].ss.toSeq
  override def toString: String = "@ArrayAnn(ss=" + ss.mkString("[", ", ", "]") + ")"
}
@ArrayAnn class ArrWithDefault
@ArrayAnn(ss=Array("foo", "bar", "splat")) class ArrWithElems

case class ClassAnn(val c: Class[_] = classOf[Any]) extends ClassfileAnnotation
@ClassAnn(c=classOf[Thread]) class ClassAnnotated

/** Tests for the various annotation finding/extracting methods. Separated out here so that all
  * the annotations and annotated classes can be directly above without being inner classes of
  * the preceeding test class.
  */
class ReflectUtilAnnotationTest extends UnitSpec {
  "ReflectionUtil.findAnnotation" should "find an annotation with all default values" in {
    ReflectionUtil.findScalaAnnotation[Ann,NoValues] shouldBe Some(Ann())
  }

  it should "find an annotation with a string literal parameter" in {
    ReflectionUtil.findScalaAnnotation[Ann,Hello] shouldBe Some(Ann(s="Hello"))
  }

  it should "find an annotation with multiple literal parameters" in {
    ReflectionUtil.findScalaAnnotation[Ann,Hello101True] shouldBe Some(Ann(s="Hello", i=101, b=true))
  }

  it should "find an annotation with a constant string concat expression" in {
    ReflectionUtil.findScalaAnnotation[Ann,Expressions] shouldBe Some(Ann(s="foobar", i=4, b=true))
  }

  it should "find an annotation with a constant reference values" in {
    ReflectionUtil.findScalaAnnotation[Ann,WithConstRefs] shouldBe Some(Ann(s=java.util.jar.JarFile.MANIFEST_NAME, i=Integer.MAX_VALUE))
  }

  it should "work with Java enums" in {
    ReflectionUtil.findScalaAnnotation[EnumAnn,EnumAnnotated] shouldBe Some(new EnumAnn())
    ReflectionUtil.findScalaAnnotation[EnumAnn,EnumAnnotatedWaiting] shouldBe Some(new EnumAnn(TimeUnit.DAYS))
    ReflectionUtil.findScalaAnnotation[InnerEnumAnn,InnerEnumAnnotated] shouldBe Some(new InnerEnumAnn(Thread.State.BLOCKED))
  }

  it should "work with an array parameter with default value" in {
    ReflectionUtil.findScalaAnnotation[ArrayAnn,ArrWithDefault] shouldBe Some(new ArrayAnn())
  }

  it should "work with an array parameter with one value" in {
    ReflectionUtil.findScalaAnnotation[ArrayAnn,ArrWithElems] shouldBe Some(new ArrayAnn(ss=Array("foo", "bar", "splat")))
  }

  it should "work with Class values in annotations" in {
    ReflectionUtil.findScalaAnnotation[ClassAnn,ClassAnnotated] shouldBe Some(new ClassAnn(c=classOf[Thread]))
  }
}
