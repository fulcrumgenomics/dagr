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

import dagr.sopt.arg
import dagr.commons.util.UnitSpec

private case class IntNoDefault(@arg v: Int)
private case class TwoParamNoDefault(@arg v: Int = 2, @arg w: Int)
private case class IntDefault(@arg v: Int = 2)
private case class DefaultWithOption(@arg w: Option[Int] = None)
private case class NoParams()
private case class StringDefault(@arg s: String = "null")
private case class ComplexDefault(@arg v: StringDefault = new StringDefault())
private case class ComplexNoDefault(@arg v: StringDefault)

private class NotCaseClass(@arg val i:Int, @arg val l:Long=123, @arg val o : Option[String] = None)
private class ParamsNotVals(@arg i:Int, @arg l:Long) {
  val x = i
  val y = l
}

private class SecondaryConstructor1(val x:Int, val y:Int) { def this(@arg a:Int) = this(a, a*2) }
private class NoAnnotation(val x:Int)
private class ConflictingNames(@arg(name="name") val x:Int, @arg(name="name") val y:Int)

class ClpReflectiveBuilderTest extends UnitSpec {

  "ClpReflectiveBuilder" should "instantiate a case-class with defaults" in {
    val t = new ClpReflectiveBuilder(classOf[IntDefault]).buildDefault()
    t.v should be (2)
    val tt = new ClpReflectiveBuilder(classOf[DefaultWithOption]).buildDefault()
    tt.w should be (None)
    new ClpReflectiveBuilder(classOf[NoParams]).buildDefault()
    new ClpReflectiveBuilder(classOf[ComplexDefault]).buildDefault()
  }

  it should "instantiate a case-class with arguments" in {
    val t = new ClpReflectiveBuilder(classOf[IntDefault]).build(List(3))
    t.v shouldBe 3
    val tt = new ClpReflectiveBuilder(classOf[DefaultWithOption]).build(List(None))
    tt.w shouldBe 'empty
    new ClpReflectiveBuilder(classOf[NoParams]).build(Nil)
    new ClpReflectiveBuilder(classOf[ComplexDefault]).build(List(new StringDefault()))
  }

  it should "throw an exception when arguments are missing when trying to instantiate a case-class" in {
    an[IllegalArgumentException] should be thrownBy new ClpReflectiveBuilder(classOf[ComplexDefault]).build(Nil)
  }

  it should "work with non-case classes" in {
    val t = new ClpReflectiveBuilder(classOf[NotCaseClass]).build(Seq(12, 456.asInstanceOf[Long], Option("Hello")))
    t.i shouldBe 12
    t.l shouldBe 456
    t.o shouldBe Some("Hello")
  }

  it should "work with constructor parameters that are not vals or vars " in {
    val t = new ClpReflectiveBuilder(classOf[ParamsNotVals]).build(Seq(911, 999))
    t.x shouldBe 911
    t.y shouldBe 999
  }

  it should "work with a secondary constructor with @arg annotations in it " in {
    val t = new ClpReflectiveBuilder(classOf[SecondaryConstructor1]).build(Seq(50))
    t.x shouldBe 50
    t.y shouldBe 100
  }

  it should "throw an IllegalStateException if there is no constructor with an @arg annotation" in {
    an[IllegalStateException] should be thrownBy new ClpReflectiveBuilder(classOf[NoAnnotation])
  }

  it should "throw an CommandLineParserInternalException when arguments have the same name" in {
    an[CommandLineParserInternalException] should be thrownBy new ClpReflectiveBuilder(classOf[ConflictingNames])
  }

  it should "throw an IllegalStateException when minElements or maxElements are given on a non-collection argument" in {
    val t = new ClpReflectiveBuilder(classOf[IntNoDefault])
    t.argumentLookup.view.foreach { arg =>
      an[IllegalStateException] should be thrownBy arg.minElements
      an[IllegalStateException] should be thrownBy arg.maxElements

    }
  }

  "ClpReflectiveBuilder.toCommandLineString" should "throw an IllegalStateException when trying to print an argument with no value" in {
    val t = new ClpReflectiveBuilder(classOf[IntNoDefault])
    t.argumentLookup.view.foreach {
      an[IllegalStateException] should be thrownBy _.toCommandLineString
    }
  }
}
