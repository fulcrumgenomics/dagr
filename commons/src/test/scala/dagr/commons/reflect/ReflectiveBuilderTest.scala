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

package dagr.commons.reflect

import dagr.commons.util.UnitSpec

/** Should work just fine. */
class NonCase(i:Int, val l: Long = 42, var s: String) {
  val ii = i
}
/** Should work just fine. */
case class CaseClass(i:Int, l: Long = 42, var s: String)
/** Should work just fine as only a single public constructor */
class MultiConstructor1(val i:Int) {
  private def this(l: Long) = this(l.toInt)
  protected def this(f: Float) = this(f.toInt)
}
/** Should work just fine as it has a primary constructor */
class MultiConstructor2(val i:Int) {
  def this(l: Long) = this(l.toInt)
}
/** Should not work since no public constructors  */
class MultiConstructor3 private(val i:Int) {
  private def this(l: Long) = this(l.toInt)
}

class NoDefaults(val i: Int)
class AllDefaults(val i: Int = 1)

/**
  * Tests for building classes using reflection
  */
class ReflectiveBuilderTest extends UnitSpec {
  "ReflectiveBuilder" should "work with non-case classes" in {
    val builder = new ReflectiveBuilder(classOf[NonCase])
    builder.argumentLookup.forField("i").get.value = 7
    builder.argumentLookup.forField("s").get.value = "foo"

    val result = builder.build()
    result.ii shouldBe 7
    result.l shouldBe 42
    result.s shouldBe "foo"

    builder.argumentLookup.forField("l").get.value = 43L
    val result2 = builder.build()
    result2.ii shouldBe 7
    result2.l shouldBe 43
    result2.s shouldBe "foo"
  }

  it should "work with case classes" in {
    val builder = new ReflectiveBuilder(classOf[CaseClass])
    builder.argumentLookup.forField("i").get.value = 7
    builder.argumentLookup.forField("s").get.value = "foo"

    val result = builder.build()
    result.i shouldBe 7
    result.l shouldBe 42
    result.s shouldBe "foo"

    builder.argumentLookup.forField("l").get.value = 43L
    val result2 = builder.build()
    result2.i shouldBe 7
    result2.l shouldBe 43
    result2.s shouldBe "foo"
  }

  it should "work with a class with multiple non-public constructors" in {
    val builder = new ReflectiveBuilder(classOf[MultiConstructor1])
    builder.argumentLookup.forField("i").foreach(_.value = 7)
    val result = builder.build()
    result.i shouldBe 7
  }

  it should "work with a class with a primary public constructors" in {
    val builder = new ReflectiveBuilder(classOf[MultiConstructor2])
    builder.argumentLookup.forField("i").foreach(_.value = 7)
    val result = builder.build()
    result.i shouldBe 7
  }

  it should "not work with private constructors" in {
    an[IllegalArgumentException] should be thrownBy new ReflectiveBuilder(classOf[MultiConstructor3])
  }

  it should "throw an exception if build() is called and some arguments don't have values" in {
    val builder = new ReflectiveBuilder(classOf[CaseClass])
    an[IllegalStateException] should be thrownBy builder.build()
  }

  "ReflectiveBuilder.buildDefault" should "throw an IllegalStateException if a constructor argument does not have a default value" in {
    val builder = new ReflectiveBuilder(clazz=classOf[NoDefaults])
    an[IllegalStateException] should be thrownBy builder.buildDefault()
  }

  it should "build a class if it has all defaults" in {
    val builder = new ReflectiveBuilder(clazz=classOf[AllDefaults])
    val instance = builder.buildDefault()
    instance.i shouldBe 1
  }
}
