/*
 * The MIT License
 *
 * Copyright (c) 2015-6 Fulcrum Genomics LLC
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
import java.nio.file.Path

import dagr.core.cmdline.{Arg, UserException}
import dagr.core.util.{PathUtil, UnitSpec}
import org.scalatest.OptionValues

object ClpArgumentTest {
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

class ClpArgumentTest extends UnitSpec with OptionValues {
  import ClpArgumentTest._

  /** Helper function to create a single ClpArgument for a class with a single constructor arg. */
  def makeClpArgument(clazz : Class[_], defaultValue: Any) : ClpArgument = {
    val arg = new ClpReflectiveBuilder(clazz).argumentLookup.view.head
    arg.value = defaultValue
    arg
  }

  "ClpArgument should" should "store a boolean field" in {
    val argument = makeClpArgument(classOf[BooleanClass], false)
    argument.isFlag should be(true)
    argument.value.get should be(false.asInstanceOf[Any])
    argument.value.get.asInstanceOf[Boolean] should be(false)
    argument.getTypeDescription shouldBe "Boolean"
    argument.optional shouldBe true
    argument.hasValue shouldBe true
    // try with.value = 
    argument.value = true
    argument.value.get should be(true.asInstanceOf[Any])
    argument.value.get.asInstanceOf[Boolean] should be(true)
    // try with setArgument
    argument.setArgument("false")
    argument.value.get should be(false.asInstanceOf[Any])
    argument.value.get.asInstanceOf[Boolean] should be(false)

    // try with setArgument with no value
    argument.isSetByUser = false // override
    argument.setArgument()
    argument.value.get should be(true.asInstanceOf[Any])
    argument.value.get.asInstanceOf[Boolean] should be(true)
    // try with setArgument with one value
    an[IllegalStateException] should be thrownBy argument.setArgument("false")
    // try with multiple values
    argument.isSetByUser = false // override
    an[UserException] should be thrownBy argument.setArgument("a", "b")
  }

  it should "store a Path" in {
    val path = PathUtil.pathTo("a", "path")
    val newPath = PathUtil.pathTo("b", "path")
    val argument = makeClpArgument(classOf[PathClass], path)
    argument.isFlag should be(false)
    argument.value.get should be(path.asInstanceOf[Any])
    argument.value.get.asInstanceOf[Path] should be(path)
    argument.getTypeDescription shouldBe "Path"
    // try with.value = 
    argument.value = newPath
    argument.value.get should be(newPath.asInstanceOf[Any])
    argument.value.get.asInstanceOf[Path] should be(newPath)
    // try with setArgument
    argument.setArgument(path.toString)
    argument.value.get should be(path.asInstanceOf[Any])
    argument.value.get.asInstanceOf[Path] should be(path)
  }

  it should "store a Seq[_]" in {
    val seq = Seq(1, 2, 3)
    val newSeq = Seq(1, 2, 3, 4)
    val argument = makeClpArgument(classOf[SeqClass], seq)
    argument.isFlag should be(false)
    argument.isCollection should be(true)
    argument.value.get should be(seq.asInstanceOf[Any])
    argument.value.get.asInstanceOf[Seq[_]] should be(seq)
    argument.value.get.asInstanceOf[Seq[_]](2) should be(3)
    argument.getTypeDescription shouldBe "Any"
    // try with.value = 
    argument.value = newSeq
    argument.value.get should be(newSeq.asInstanceOf[Any])
    argument.value.get.asInstanceOf[Seq[_]] should be(newSeq)
    argument.value.get.asInstanceOf[Seq[_]](3) should be(4)
    // try with setArgument
    argument.setArgument(seq.map(i => i.toString):_*)
    argument.value.get.asInstanceOf[Seq[_]].toList should be(seq.map(_.toString))
    argument.value.get.asInstanceOf[Seq[_]](2) should be("3")
  }

  it should "store a Seq[Int]" in {
    val seq = Seq[Int](1, 2, 3)
    val newSeq = Seq[Int](1, 2, 3, 4)
    val argument = makeClpArgument(classOf[SeqIntClass], seq)
    argument.isFlag should be(false)
    argument.isCollection should be(true)
    argument.value.get should be(seq.asInstanceOf[Any])
    argument.value.get.asInstanceOf[Seq[_]] should be(seq)
    argument.value.get.asInstanceOf[Seq[_]](2) should be(3)
    argument.getTypeDescription shouldBe "Int"
    // try with.value = 
    argument.value = newSeq
    argument.value.get should be(newSeq.asInstanceOf[Any])
    argument.value.get.asInstanceOf[Seq[_]] should be(newSeq)
    argument.value.get.asInstanceOf[Seq[_]](3) should be(4)
    // try with setArgument
    argument.setArgument(seq.map(i => i.toString):_*)
    argument.value.get.asInstanceOf[Seq[_]].toList should be(seq) // major difference #1 : this is an int
    argument.value.get.asInstanceOf[Seq[_]](2) should be(3) // major difference #2 : this is an int
  }

  it should "store a Option[PathToNowhere]" in {
    val aPath: Option[PathToNowhere] = Some(PathUtil.pathTo("a", "path"))
    val aNewPath = Option(PathUtil.pathTo("b", "path"))
    val argument = makeClpArgument(classOf[PathToNowhereOptionClass], aPath)
    argument.isFlag should be(false)
    argument.isCollection should be(false)
    argument.value.get should be(aPath.asInstanceOf[Any])
    argument.value.get.asInstanceOf[Option[PathToNowhere]] should be(aPath)
    argument.getTypeDescription shouldBe "PathToNowhere"
    // try with.value = 
    argument.value = aNewPath
    argument.value.get shouldBe aNewPath.asInstanceOf[Any]
    argument.value.get.asInstanceOf[Option[PathToNowhere]] should be (aNewPath)
    argument.value.get.asInstanceOf[Option[PathToNowhere]].value should be (aNewPath.get)
    // try with setArgument
    argument.setArgument(aPath.get.toString)
    argument.value.get shouldBe aPath.asInstanceOf[Any]
    argument.value.get.asInstanceOf[Option[PathToNowhere]] should be (aPath)
    argument.value.get.asInstanceOf[Option[PathToNowhere]].value should be (aPath.get)
  }

  it should "get the description correctly for type aliases" in {
    val parent = new PathToNowhereOptionClass(Some(PathUtil.pathTo("a", "path")))
    val field = parent.getClass.getDeclaredFields.head
    val annotation = field.getAnnotation(classOf[Arg])
    val argument = makeClpArgument(classOf[PathToNowhereOptionClass], null)
    argument.getTypeDescription shouldBe "PathToNowhere"
  }

  it should "store an argument without an annotation" in {
    val argument = makeClpArgument(classOf[NoAnnotation], false)
    argument.isFlag should be(true)
    argument.value.get should be(false.asInstanceOf[Any])
    argument.value.get.asInstanceOf[Boolean] should be(false)
    argument.getTypeDescription shouldBe "Boolean"
    argument.optional shouldBe true
    argument.hasValue shouldBe true
    // try with.value = 
    argument.value = true
    argument.value.get should be(true.asInstanceOf[Any])
    argument.value.get.asInstanceOf[Boolean] should be(true)
    // try with setArgument
    argument.setArgument("false")
    argument.value.get should be(false.asInstanceOf[Any])
    argument.value.get.asInstanceOf[Boolean] should be(false)
  }

  it should "throw an IllegalStateException when creating an argument without an annotation with no default" in {
    an[IllegalStateException] should be thrownBy new ClpReflectiveBuilder(classOf[NoAnnotationNoDefault]).argumentLookup.view.head
  }
}


