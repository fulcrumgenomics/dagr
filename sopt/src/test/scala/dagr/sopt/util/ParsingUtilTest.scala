/*
 * The MIT License
 *
 * Copyright (c) $year Fulcrum Genomics
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
 *
 */
package dagr.sopt.util

import java.lang.reflect.Field

import dagr.commons.util.UnitSpec
import dagr.sopt.cmdline.CommandLineException
import dagr.sopt.cmdline.testing.simple.CommandLineProgram
import org.scalatest.{OptionValues, PrivateMethodTester}

object ParsingUtilTest {
  /** Helper to get the first declared field */
  def getField(clazz: Class[_]): Field = clazz.getDeclaredFields.head

}

class ParsingUtilTest extends UnitSpec with OptionValues with PrivateMethodTester {

  private val simplePackageList = List("dagr.sopt.cmdline.testing.simple")

  import ParsingUtil._

  it should "find classes that extend CommandLineProgram with the @CLP annotation" in {
    val map = findClpClasses[CommandLineProgram](simplePackageList)

    import dagr.sopt.cmdline.testing.simple._
    map should contain key classOf[InClass]
    map should contain key classOf[InClass2]
    map should not contain key (classOf[NoOpCommandLineProgram])
    map should not contain key (classOf[OutClass])
    map should not contain key (classOf[Out2Class])
    map should not contain key (classOf[Out3Class])
  }

  it should "throw a CommandLineException when two Pipelines have the same simple name" in {
    an[CommandLineException] should be thrownBy findClpClasses[CommandLineProgram](simplePackageList, includeHidden = true)
  }

//  it should "find classes that are missing the annotation @CLP" in {
//    an[BadAnnotationException] should be thrownBy getClassToPropertyMap(List("dagr.core.cmdline.parsing.testing.missing"))
//  }

//  it should "get the type for a field" in {
//    getFieldClass("list", classOf[WithList], unwrapContainers = false) shouldBe classOf[List[_]]
//    getFieldClass("list", classOf[WithIntList], unwrapContainers = false) shouldBe classOf[List[_]]
//    getFieldClass("list", classOf[WithJavaCollection], unwrapContainers = false) shouldBe classOf[util.Collection[_]]
//    getFieldClass("v",    classOf[WithOption], unwrapContainers = false) shouldBe classOf[Option[_]]
//    getFieldClass("v",    classOf[WithIntOption], unwrapContainers = false) shouldBe classOf[Option[_]]
//    getFieldClass("v",    classOf[WithInt], unwrapContainers = false) shouldBe classOf[Int]
//    getFieldClass("path", classOf[WithPathToBamOption], unwrapContainers = false) shouldBe classOf[Option[_]]
//    an[CommandLineException] should be thrownBy getFieldClass("doesNotExist", classOf[WithInt], unwrapContainers = false)
//  }
//
//  it should "get the generic type for a field" in {
//    getFieldClass("list", classOf[WithList]) shouldBe classOf[Any]
//    getFieldClass("list", classOf[WithIntList]) shouldBe classOf[Int]
//    getFieldClass("list", classOf[WithJavaCollection]) shouldBe classOf[Any]
//    getFieldClass("v", classOf[WithOption]) shouldBe classOf[Any]
//    getFieldClass("v", classOf[WithIntOption]) shouldBe classOf[Int]
////    getFieldClass("v", classOf[WithInt]) shouldBe 'empty
//    getFieldClass("path", classOf[WithPathToBamOption]) shouldBe classOf[PathToBam]
//    an[CommandLineException] should be thrownBy getFieldClass("doesNotExist", classOf[WithInt])
//    an[CommandLineException] should be thrownBy getFieldClass("map", classOf[WithMap]) // multiple generic types!
//  }

//  it should "gets the underlying type for a field" in {
//    import dagr.core.cmdline.parsing.testing.fields._
//
//    getUnitClass("list", classOf[WithList]) shouldBe classOf[Any]
//    getUnitClass("list", classOf[WithIntList]) shouldBe classOf[java.lang.Integer]
//    getUnitClass("list", classOf[WithJavaCollection]) shouldBe classOf[Any]
//    getUnitClass("v",    classOf[WithOption]) shouldBe classOf[Any]
//    getUnitClass("v",    classOf[WithIntOption]) shouldBe classOf[java.lang.Integer]
//    getUnitClass("v",    classOf[WithInt]) shouldBe classOf[java.lang.Integer]
//    getUnitClass("path", classOf[WithPathToBamOption]) shouldBe classOf[Path]
//  }
}
