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
package dagr.core.cmdline.parsing
import java.nio.file.{Path, Paths}

import dagr.core.cmdline.Arg
import dagr.core.util.UnitSpec
import org.scalatest.{OptionValues}

object ArgumentDefinitionTest {

  type PathToNowhere = java.nio.file.Path

  case class BooleanClass(@Arg var aBool: Boolean)

  case class PathClass(@Arg var aPath: Path)

  case class SeqClass(@Arg var aSeqOfUnknown: Seq[_])

  case class SeqIntClass(@Arg var aSeqOfInt: Seq[Int])

  case class PathToNowhereOptionClass(@Arg var anOptionPath: Option[PathToNowhere])

  case class SeqWithDefaults(@Arg var aSeq: Seq[_] = Seq(1, 2, 3))

  case class NoAnnotation(var aVar: Boolean = false)

  case class NoAnnotationNoDefault(var Boolean: Int)

}

class ArgumentDefinitionTest extends UnitSpec with OptionValues {
  import ArgumentDefinitionTest._

  /** Helper function to create a single ArgumentDefinition for a class with a single constructor arg. */
  def makeArgDef(clazz : Class[_], defaultValue: Any) : ArgumentDefinition = {
    val arg = new ReflectionHelper(clazz).argumentLookup.view.head
    arg.value = Option(defaultValue)
    arg
  }

  "ArgumentDefinition should" should "store a boolean field" in {
    val argumentDefinition = makeArgDef(classOf[BooleanClass], false)
    argumentDefinition.isFlag should be(true)
    argumentDefinition.value.get should be(false.asInstanceOf[Any])
    argumentDefinition.value.get.asInstanceOf[Boolean] should be(false)
    argumentDefinition.getTypeDescription shouldBe "Boolean"
    argumentDefinition.optional shouldBe true
    argumentDefinition.hasValue shouldBe true
    // try with setFieldValue
    argumentDefinition.setFieldValue(true)
    argumentDefinition.value.get should be(true.asInstanceOf[Any])
    argumentDefinition.value.get.asInstanceOf[Boolean] should be(true)
    // try with setArgument
    argumentDefinition.setArgument(List("false"))
    argumentDefinition.value.get should be(false.asInstanceOf[Any])
    argumentDefinition.value.get.asInstanceOf[Boolean] should be(false)
  }

  it should "store a Path" in {
    val path = Paths.get("a", "path")
    val newPath = Paths.get("b", "path")
    val argumentDefinition = makeArgDef(classOf[PathClass], path)
    argumentDefinition.isFlag should be(false)
    argumentDefinition.value.get should be(path.asInstanceOf[Any])
    argumentDefinition.value.get.asInstanceOf[Path] should be(path)
    argumentDefinition.getTypeDescription shouldBe "Path"
    // try with setFieldValue
    argumentDefinition.setFieldValue(newPath)
    argumentDefinition.value.get should be(newPath.asInstanceOf[Any])
    argumentDefinition.value.get.asInstanceOf[Path] should be(newPath)
    // try with setArgument
    argumentDefinition.setArgument(List(path.toString))
    argumentDefinition.value.get should be(path.asInstanceOf[Any])
    argumentDefinition.value.get.asInstanceOf[Path] should be(path)
  }

  // TODO: review with Nils; this is failing but appears to be doing what is expected
  it should "store a Seq[_]" in {
    val seq = Seq(1, 2, 3)
    val newSeq = Seq(1, 2, 3, 4)
    val argumentDefinition = makeArgDef(classOf[SeqClass], seq)
    argumentDefinition.isFlag should be(false)
    argumentDefinition.isCollection should be(true)
    argumentDefinition.value.get should be(seq.asInstanceOf[Any])
    argumentDefinition.value.get.asInstanceOf[Seq[_]] should be(seq)
    argumentDefinition.value.get.asInstanceOf[Seq[_]](2) should be(3)
    argumentDefinition.getTypeDescription shouldBe "Any"
    // try with setFieldValue
    argumentDefinition.setFieldValue(newSeq)
    argumentDefinition.value.get should be(newSeq.asInstanceOf[Any])
    argumentDefinition.value.get.asInstanceOf[Seq[_]] should be(newSeq)
    argumentDefinition.value.get.asInstanceOf[Seq[_]](3) should be(4)
    // try with setArgument
    argumentDefinition.setArgument(seq.map(i => i.toString).toList)
    argumentDefinition.value.get.asInstanceOf[Seq[_]].toList should be(seq.map(_.toString))
    argumentDefinition.value.get.asInstanceOf[Seq[_]](2) should be("3")
  }

  // TODO: review with Nils; this is failing but appears to be doing what is expected
  it should "store a Seq[Int]" in {
    val seq = Seq[Int](1, 2, 3)
    val newSeq = Seq[Int](1, 2, 3, 4)
    val argumentDefinition = makeArgDef(classOf[SeqIntClass], seq)
    argumentDefinition.isFlag should be(false)
    argumentDefinition.isCollection should be(true)
    argumentDefinition.value.get should be(seq.asInstanceOf[Any])
    argumentDefinition.value.get.asInstanceOf[Seq[_]] should be(seq)
    argumentDefinition.value.get.asInstanceOf[Seq[_]](2) should be(3)
    argumentDefinition.getTypeDescription shouldBe "Int"
    // try with setFieldValue
    argumentDefinition.setFieldValue(newSeq)
    argumentDefinition.value.get should be(newSeq.asInstanceOf[Any])
    argumentDefinition.value.get.asInstanceOf[Seq[_]] should be(newSeq)
    argumentDefinition.value.get.asInstanceOf[Seq[_]](3) should be(4)
    // try with setArgument
    argumentDefinition.setArgument(seq.map(i => i.toString).toList)
    argumentDefinition.value.get.asInstanceOf[Seq[_]].toList should be(seq) // major difference #1 : this is an int
    argumentDefinition.value.get.asInstanceOf[Seq[_]](2) should be(3) // major difference #2 : this is an int
  }

  it should "store a Option[PathToNowhere]" in {
    val aPath: Option[PathToNowhere] = Some(Paths.get("a", "path"))
    val aNewPath = Option(Paths.get("b", "path"))
    val argumentDefinition = makeArgDef(classOf[PathToNowhereOptionClass], aPath)
    argumentDefinition.isFlag should be(false)
    argumentDefinition.isCollection should be(false)
    argumentDefinition.value.get should be(aPath.asInstanceOf[Any])
    argumentDefinition.value.get.asInstanceOf[Option[PathToNowhere]] should be(aPath)
    argumentDefinition.getTypeDescription shouldBe "PathToNowhere"
    // try with setFieldValue
    argumentDefinition.setFieldValue(aNewPath)
    argumentDefinition.value.get shouldBe aNewPath.asInstanceOf[Any]
    argumentDefinition.value.get.asInstanceOf[Option[PathToNowhere]] should be (aNewPath)
    argumentDefinition.value.get.asInstanceOf[Option[PathToNowhere]].value should be (aNewPath.get)
    // try with setArgument
    argumentDefinition.setArgument(List(aPath.get.toString))
    argumentDefinition.value.get shouldBe aPath.asInstanceOf[Any]
    argumentDefinition.value.get.asInstanceOf[Option[PathToNowhere]] should be (aPath)
    argumentDefinition.value.get.asInstanceOf[Option[PathToNowhere]].value should be (aPath.get)
  }

  it should "get the description correctly for type aliases" in {
    val parent = new PathToNowhereOptionClass(Some(Paths.get("a", "path")))
    val field = parent.getClass.getDeclaredFields.head
    val annotation = field.getAnnotation(classOf[Arg])
    val argumentDefinition = makeArgDef(classOf[PathToNowhereOptionClass], null)
    argumentDefinition.getTypeDescription shouldBe "PathToNowhere"
  }

  it should "store an argument without an annotation" in {
    val argumentDefinition = makeArgDef(classOf[NoAnnotation], false)
    argumentDefinition.isFlag should be(true)
    argumentDefinition.value.get should be(false.asInstanceOf[Any])
    argumentDefinition.value.get.asInstanceOf[Boolean] should be(false)
    argumentDefinition.getTypeDescription shouldBe "Boolean"
    argumentDefinition.optional shouldBe true
    argumentDefinition.hasValue shouldBe true
    // try with setFieldValue
    argumentDefinition.setFieldValue(true)
    argumentDefinition.value.get should be(true.asInstanceOf[Any])
    argumentDefinition.value.get.asInstanceOf[Boolean] should be(true)
    // try with setArgument
    argumentDefinition.setArgument(List("false"))
    argumentDefinition.value.get should be(false.asInstanceOf[Any])
    argumentDefinition.value.get.asInstanceOf[Boolean] should be(false)
  }

  it should "throw an IllegalStateException when creating an argument without an annotation with no default" in {
    an[IllegalStateException] should be thrownBy new ReflectionHelper(classOf[NoAnnotationNoDefault]).argumentLookup.view.head
  }
}


