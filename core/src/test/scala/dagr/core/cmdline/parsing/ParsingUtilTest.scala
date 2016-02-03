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

import java.lang.reflect.Field
import java.nio.file.Paths

import dagr.core.util.{ReflectionUtil, LogLevel, UnitSpec}
import org.scalatest.{OptionValues, PrivateMethodTester}

object ParsingUtilTest {
  /** Helper to get the first declared field */
  def getField(clazz: Class[_]): Field = clazz.getDeclaredFields.head

}

class ParsingUtilTest extends UnitSpec with OptionValues with PrivateMethodTester {

  import ParsingUtil._

  "ParsingUtil" should "find classes that extend Pipeline with the @CLP annotation" in {
    val map = findPipelineClasses(List("dagr.core.cmdline.parsing.testing.simple"))

    import dagr.core.cmdline.parsing.testing.simple._
    map should contain key classOf[InClass]
    map should contain key classOf[InClass2]
    map should not contain key (classOf[NoOpCommandLineTask])
    map should not contain key (classOf[OutClass])
    map should not contain key (classOf[Out2Class])
    map should not contain key (classOf[Out3Class])
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

  it should "construct an Int from a string for an Int field" in {
    //canBeMadeFromString("v", classOf[Int], classOf[WithInt]) shouldBe true
    constructFromString(classOf[Int], classOf[Int], "1") shouldBe 1
  }

  it should "construct an String from a string for a String field" in {
    //canBeMadeFromString("s", classOf[String], classOf[WithString]) shouldBe true
    constructFromString(classOf[String], classOf[String], "str") shouldBe "str"
  }

  it should "construct an Option[_] from a string for an Option[_] field" in {
    //canBeMadeFromString("v", classOf[Option[_]], classOf[WithOption]) shouldBe true
    constructFromString(classOf[Option[_]], classOf[String], "1") shouldBe Some("1")
  }

  it should "construct an Option[Int] from a string for an Option[Int] field" in {
    //canBeMadeFromString("v", classOf[Option[Int]], classOf[WithIntOption]) shouldBe true
    constructFromString(classOf[Option[Int]], classOf[Int], "1") shouldBe Some(1)
  }

  it should "construct an Any from a string for a List[_] field" in {
    //canBeMadeFromString("list", classOf[List[_]], classOf[WithList]) shouldBe true
    constructFromString(classOf[List[_]], classOf[String], "1", "2") shouldBe List("1", "2")
  }

  it should "construct an Int from a string for a List[Int] field" in {
    //canBeMadeFromString("list", classOf[List[Int]], classOf[WithIntList]) shouldBe true
    constructFromString(classOf[List[Int]], classOf[Int], "1", "2") shouldBe List(1, 2)
  }

  it should "construct an Any from a string for a java.util.Collection[_] field" in {
    //canBeMadeFromString("list", classOf[java.util.Collection[_]], classOf[WithJavaCollection]) shouldBe true
    val collection: java.util.Collection[_] = constructFromString(classOf[java.util.Collection[_]], classOf[String], "1", "2").asInstanceOf[java.util.Collection[_]]
    collection should have size 2
    collection should contain ("1")
    collection should contain ("2")
  }

  it should "construct an Any from a string for a java.util.Set[_] field" in {
    //canBeMadeFromString("list", classOf[java.util.Set[_]], classOf[WithJavaCollection]) shouldBe true
    val collection: java.util.Collection[_] = constructFromString(classOf[java.util.Set[_]], classOf[String], "1", "2").asInstanceOf[java.util.Collection[_]]
    collection should have size 2
    collection should contain ("1")
    collection should contain ("2")
  }

  it should "construct an Path from a string for a PathToBam field" in {
    type PathToSomething = java.nio.file.Path
    //canBeMadeFromString("path", classOf[PathToBam], classOf[WithPathToBam]) shouldBe true
    constructFromString(classOf[PathToSomething], classOf[PathToSomething], Paths.get("b", "c").toString) shouldBe Paths.get("b", "c")
  }

  it should "construct an String from a string for a String field in a child class" in {
    //canBeMadeFromString("t", classOf[String], classOf[WithStringChild]) shouldBe true
    constructFromString(classOf[String], classOf[String], "str") shouldBe "str"
  }

  it should "construct an Enum value from a string for a Enum field" in {
    //canBeMadeFromString("verbosity", classOf[LogLevel], classOf[WithEnum]) shouldBe true
    constructFromString(classOf[LogLevel], classOf[LogLevel], "Debug") shouldBe LogLevel.Debug
  }

  it should "not construct an Option[Option[String]] from a string" in {
    class OptionOptionString(var v: Option[Option[String]])
    //canBeMadeFromString("v", classOf[Option[Option[String]]], classOf[OptionOptionString]) shouldBe false
    an[Exception] should be thrownBy constructFromString( classOf[Option[Option[String]]], classOf[Option[String]], "str")
  }

  it should "not construct an Map[String,String] from a string" in {
    class MapString(var v: Map[String, String])
    //canBeMadeFromString("v", classOf[Map[String, String], classOf[OptionOptionString]) shouldBe false
    an[Exception] should be thrownBy constructFromString(classOf[Map[String, String]], classOf[String], "str")
  }

  it should "construct a Set and value from a string for a Set[_] field" in {
    //canBeMadeFromString("set", classOf[Set[_]], classOf[SetClass]) shouldBe true
    constructFromString(classOf[Set[_]], classOf[String], "value") shouldBe Set("value")
  }

  it should "construct a Seq and value from a string for a Seq[_] field" in {
    //canBeMadeFromString("seq", classOf[Set[_]], classOf[SeqClass]) shouldBe true
    constructFromString(classOf[Seq[_]], classOf[String], "value") shouldBe Seq("value")
  }

  it should "not treat the argument value 'null' as anything special" in {
    //canBeMadeFromString("seq", classOf[Set[_]], classOf[SeqClass]) shouldBe true
    constructFromString(classOf[Seq[_]], classOf[String], "prefix", "null", "suffix") shouldBe Seq("prefix", "null", "suffix")
  }
}
