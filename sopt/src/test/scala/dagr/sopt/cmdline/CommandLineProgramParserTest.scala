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

import dagr.commons.util.{CaptureSystemStreams, UnitSpec, LogLevel}
import dagr.commons.reflect.{ReflectionException, GoodEnum, BadEnum}
import dagr.sopt._
import dagr.sopt.cmdline.ClpArgumentDefinitionPrinting._

import dagr.sopt.cmdline.testing.clps.{CommandLineProgramShortArg, CommandLineProgramReallyLongArg}
import dagr.sopt.util.TermCode
import org.scalatest.{BeforeAndAfterAll, Inside, OptionValues}

import scala.collection.Map
import scala.collection.immutable.HashMap
import scala.util.Success

////////////////////////////////////////////////////////////////////////////////
// Holds the classes we need for testing.  These cannot be inner classes.
////////////////////////////////////////////////////////////////////////////////

@clp(description = "", group = classOf[TestGroup], hidden = true)
private class CommandLineProgramTesting(@arg var aStringSomething: String = "Default")

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassNoParams()
extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithInt
(@arg var anInt: Int) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithIntDefault
(@arg var anInt: Int = 2) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithString
(@arg var aString: String) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithStringDefault
(@arg var aString: String = "string") extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithNullString
(@arg var aString: String = null) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithBoolean
(@arg var aBoolean: Boolean) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithBooleanDefault
(@arg var aBoolean: Boolean = true) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithJavaInteger
(@arg var aJavaInteger: java.lang.Integer) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithJavaIntegerWithDefault
(@arg var aJavaInteger: java.lang.Integer = 2) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithJavaBoolean
(@arg var aJavaBoolean: java.lang.Boolean) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithJavaBooleanWithDefault
(@arg var aJavaBoolean: java.lang.Boolean = true) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithOptionWithSomeDefault
(@arg var anOption: Option[_] = Some(2)) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithOptionWithNoneDefault
(@arg var anOption: Option[_] = None) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithOptionWithNoneDefaultAndOptional
(@arg var anOption: Option[_] = None) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithIntOptionWithSomeDefault
(@arg var anOption: Option[Int] = Some(2)) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithIntOptionWithNoneDefault
(@arg var anOption: Option[Int] = None) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithList
(@arg var aList: List[_] = Nil) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithIntList
(@arg var aList: List[Int] = Nil) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithSeq
(@arg var aSeq: scala.collection.Seq[_] = Nil) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithCollection
(@arg var aCollection: java.util.Collection[_] = new java.util.ArrayList[Any]()) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithJavaSet
(@arg var aSet: java.util.Set[_] = new java.util.HashSet[Any]()) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithEnum
(@arg var anEnum: LogLevel = LogLevel.Debug) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class GeneralTestingProgram
(
  @arg(minElements = 2, maxElements = 2) var stringSet: scala.collection.Set[String] = null, // also tests if we can initialize it with a set
  @arg(minElements = 2, maxElements = 2) var intSet: scala.collection.Set[Int] = null, // also tests if we can initialize it with a set
  @arg(flag = "i"                  )     var intArg: Int = 1, // tests short name
  @arg                                   var enumArg: LogLevel = LogLevel.Debug, // tests enums
  @arg                                   var stringList: List[String] = List[String](), // tests non empty??? TODO
  @arg                                   var flag: Boolean = false
) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class NoArguments()
extends CommandLineProgramTesting


@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class OptionalOnlyArguments
(@arg var arg: String = "42") extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class RequiredOnlyArguments
(@arg var arg: String = null) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class CollectionRequired(@arg var ints: List[Int] = Nil)

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class LogLevelEnumProgram(@arg var verbosity: LogLevel = LogLevel.Info)

@clp(description = "", group = classOf[TestGroup], hidden = true)
private[cmdline] case class MutexArguments
(
  @arg(mutex = Array("M", "N", "Y", "Z")) var A: String = null,
  @arg(mutex = Array("M", "N", "Y", "Z")) var B: String = null,
  @arg(mutex = Array("A", "B", "Y", "Z")) var M: String = null,
  @arg(mutex = Array("A", "B", "Y", "Z")) var N: String = null,
  @arg(mutex = Array("A", "B", "M", "N")) var Y: String = null,
  @arg(mutex = Array("A", "B", "M", "N")) var Z: String = null
)

@clp(description = "", group = classOf[TestGroup], hidden = true)
private[cmdline] case class MissingMutexArguments
(
  @arg(mutex = Array("B")) var A: String = null,
  @arg(mutex = Array("C")) var B: String = null // C should not be found
)

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class MapCollectionArgument
(
  @arg var map: Map[String,String] = new HashMap[String,String]()
)

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class CollectionWithDefaults
(
  @arg var seq: Seq[String] = Seq[String]("A", "B", "C")
)

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class FlagClass
(
  @arg var flag1: Boolean = false,
  @arg var flag2: Boolean = true
)

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class PrivateArguments
(
  @arg private var privateFlag: Boolean = false,
  @arg private var privateSeq: Seq[_] = Nil

) {
  def getPrivateFlag: Boolean = privateFlag
  def getPrivateSeq: Seq[_] = privateSeq
}

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class ValFlagClass
(
  @arg flag: Boolean = false
) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class NoVarFlagClass
(
  @arg flag: Boolean = false
) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class SensitiveArgTestingProgram
(
  @arg(sensitive = true) var flag: Boolean = false
) extends CommandLineProgramTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class UninitializedCollectionArguments
(
  @arg var seq: Seq[_] = Nil,
  @arg var set: Set[_] = Set.empty,
  @arg var collection: java.util.Collection[_] = null
)

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class NoArgAnnotationWithDefaults
(
  var anInt: Int = 2,
  var aBoolean: Boolean = true
)

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class NoArgAnnotationWithoutDefaults
(
  var anInt: Int,
  var aBoolean: Boolean
)

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class NoDefaultValueStringsClass
(
  @arg var a: String,
  @arg var b: Option[String],
  @arg var c: List[String],
  @arg var d: Set[String],
  @arg var e: java.util.Collection[String]
)

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class EmptyDefaultValueStringsClass
(
  @arg var a: String,
  @arg var b: Option[String] = None,
  @arg var c: List[String] = Nil,
  @arg var d: Set[String] = Set.empty,
  @arg var e: java.util.Collection[String] = new java.util.ArrayList[String]()
)

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class NonEmptyDefaultValueStringsClass
(
  @arg var a: Option[String] = Some("unique-value-a"),
  @arg var b: String = "unique-value-b",
  @arg var c: List[String] = List[String]("unique-value-c"),
  @arg var d: Set[String] = Set[String]("unique-value-d"),
  @arg var e: java.util.Collection[String] = {
    val defaultCollection = new java.util.ArrayList[String]()
    defaultCollection.add("unique-value-e")
    defaultCollection
  }
)

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class GoodEnumClass
(
  @arg var a: GoodEnum
)

@clp(description = "", group = classOf[TestGroup], hidden = true)
private case class BadEnumClass
(
  @arg var a: BadEnum
)
@clp(
  description =
    """
      |<START>
      |Multi
      |line
      |description
      |<END>
    """",
  group = classOf[TestGroup],
  hidden = true
)
private case class SomeDescription (@arg var a: Int = 2)

////////////////////////////////////////////////////////////////////////////////
// End of Testing CLP classes
////////////////////////////////////////////////////////////////////////////////

class CommandLineProgramParserTest extends UnitSpec with OptionValues with Inside
with CommandLineParserStrings with CaptureSystemStreams with BeforeAndAfterAll {

  private val prevPrintColor = TermCode.printColor
  override protected def beforeAll(): Unit = TermCode.printColor = false
  override protected def afterAll(): Unit = TermCode.printColor = prevPrintColor

  override def commandLineName: String = "???"

  // Create a new parser for each test
  def parser[T](clazz: Class[T]) : CommandLineProgramParser[T] = new CommandLineProgramParser(clazz)

  // How many special argument names are there? Will need to be changed if SpecialArgumentsCollection changes
  private val numSpecialArguments: Int = 3


  // required so that colors are not in our usage messages
  "CommandLineProgramParserTest" should "have color status be false" in {
    // this should be set in the build.sbt
    TermCode.printColor shouldBe false
  }

  "CommandLineProgramParser.createArgumentDefinition" should "create a class with a default int parameter" in {
    val lookup = parser(classOf[ClassWithIntDefault]).argumentLookup
    lookup.names should have size (1+numSpecialArguments)
    lookup.names should contain ("an-int")
    val arg = lookup.forArg("an-int").get
    arg.value.get shouldBe 2
    arg.argumentType shouldBe classOf[Int]
    arg.hasValue shouldBe true
  }

  it should "create a class with a default non-null string parameter" in {
    val lookup = parser(classOf[ClassWithStringDefault]).argumentLookup
    lookup.names should have size (1+numSpecialArguments)
    lookup.names should contain ("a-string")
    val argumentDefinition: ClpArgument = lookup.forArg("a-string").get
    argumentDefinition.value.get shouldBe "string"
    argumentDefinition.argumentType shouldBe classOf[java.lang.String]
    argumentDefinition.hasValue shouldBe true
  }

  it should "create a class with a default null-string parameter" in {
    val lookup = parser(classOf[ClassWithNullString]).argumentLookup
    lookup.names should have size (1+numSpecialArguments)
    lookup.names should contain ("a-string")
    val argumentDefinition: ClpArgument = lookup.forArg("a-string").get
    argumentDefinition.value shouldBe None
    argumentDefinition.argumentType shouldBe classOf[java.lang.String]
    argumentDefinition.hasValue shouldBe false
  }

  it should "create a class with a default boolean parameter" in {
    val lookup = parser(classOf[ClassWithBooleanDefault]).argumentLookup
    lookup.names should have size (1+numSpecialArguments)
    lookup.names should contain ("a-boolean")
    val argumentDefinition: ClpArgument = lookup.forArg("a-boolean").get
    argumentDefinition.value.get shouldBe true
    argumentDefinition.argumentType shouldBe classOf[Boolean]
    argumentDefinition.hasValue shouldBe true
  }

  it should "create a class with a default java.lang.Integer parameter" in {
    val lookup = parser(classOf[ClassWithJavaIntegerWithDefault]).argumentLookup
    lookup.names should have size (1+numSpecialArguments)
    lookup.names should contain ("a-java-integer")
    val argumentDefinition: ClpArgument = lookup.forArg("a-java-integer").get
    argumentDefinition.value.get shouldBe 2
    argumentDefinition.argumentType shouldBe classOf[java.lang.Integer]
    argumentDefinition.hasValue shouldBe true
  }

  it should "create a class with a default java.lang.Boolean parameter" in {
    val lookup = parser(classOf[ClassWithJavaBooleanWithDefault]).argumentLookup
    lookup.names should have size (1+numSpecialArguments)
    lookup.names should contain ("a-java-boolean")
    val argumentDefinition: ClpArgument = lookup.forArg("a-java-boolean").get
    argumentDefinition.value.get shouldBe true
    argumentDefinition.argumentType shouldBe classOf[java.lang.Boolean]
    argumentDefinition.hasValue shouldBe true
  }

  it should "create a class with a default Some-Option parameter" in {
    val lookup = parser(classOf[ClassWithOptionWithSomeDefault]).argumentLookup
    lookup.names should have size (1+numSpecialArguments)
    lookup.names should contain ("an-option")
    val argumentDefinition: ClpArgument = lookup.forArg("an-option").get
    argumentDefinition.value.get shouldBe Some(2)
    argumentDefinition.argumentType shouldBe classOf[Option[_]]
    argumentDefinition.unitType shouldBe classOf[Any]
    argumentDefinition.hasValue shouldBe true
  }

  it should "create a class with a default None-Option parameter but optional annotation" in {
    val lookup = parser(classOf[ClassWithOptionWithNoneDefaultAndOptional]).argumentLookup
    lookup.names should have size (1+numSpecialArguments)
    lookup.names should contain ("an-option")
    val argumentDefinition: ClpArgument = lookup.forArg("an-option").get
    argumentDefinition.value.get.asInstanceOf[Option[_]] shouldBe 'empty
    argumentDefinition.argumentType shouldBe classOf[Option[_]]
    argumentDefinition.hasValue shouldBe true
  }

  it should "create a class with a default Some-Option[Int] parameter" in {
    val lookup = parser(classOf[ClassWithIntOptionWithSomeDefault]).argumentLookup
    lookup.names should have size (1+numSpecialArguments)
    lookup.names should contain ("an-option")
    val argumentDefinition: ClpArgument = lookup.forArg("an-option").get
    argumentDefinition.value.get shouldBe Some(2)
    argumentDefinition.argumentType shouldBe classOf[Option[_]]
    argumentDefinition.unitType shouldBe classOf[Int]
    argumentDefinition.hasValue shouldBe true
  }

  it should "create a class with a default List[_] parameter" in {
    val lookup = parser(classOf[ClassWithList]).argumentLookup
    lookup.names should have size (1+numSpecialArguments)
    lookup.names should contain ("a-list")
    val argumentDefinition: ClpArgument = lookup.forArg("a-list").get
    argumentDefinition.value.get shouldBe Nil
    argumentDefinition.argumentType shouldBe classOf[List[_]]
    argumentDefinition.unitType shouldBe classOf[Any]
    argumentDefinition.hasValue shouldBe false
  }

  it should "create a class with a default List[Int] parameter" in {
    val lookup = parser(classOf[ClassWithIntList]).argumentLookup
    lookup.names should have size (1+numSpecialArguments)
    lookup.names should contain ("a-list")
    val argumentDefinition: ClpArgument = lookup.forArg("a-list").get
    argumentDefinition.value.get shouldBe Nil
    argumentDefinition.argumentType shouldBe classOf[List[Int]]
    argumentDefinition.unitType shouldBe classOf[Int]
    argumentDefinition.hasValue shouldBe false
  }

  it should "create a class with a default Seq[_] parameter" in {
    val lookup = parser(classOf[ClassWithSeq]).argumentLookup
    lookup.names should have size (1+numSpecialArguments)
    lookup.names should contain ("a-seq")
    val argumentDefinition: ClpArgument = lookup.forArg("a-seq").get
    argumentDefinition.value.get shouldBe Nil
    argumentDefinition.argumentType shouldBe classOf[Seq[_]]
    argumentDefinition.unitType shouldBe classOf[Any]
    argumentDefinition.hasValue shouldBe false
  }

  it should "create a class with a default java.util.Collection[_] parameter" in {
    val lookup = parser(classOf[ClassWithCollection]).argumentLookup
    lookup.names should have size (1+numSpecialArguments)
    lookup.names should contain ("a-collection")
    val argumentDefinition: ClpArgument = lookup.forArg("a-collection").get
    argumentDefinition.value.get.asInstanceOf[java.util.Collection[_]].size shouldBe 0
    argumentDefinition.argumentType shouldBe classOf[java.util.Collection[_]]
    argumentDefinition.unitType shouldBe classOf[Any]
    argumentDefinition.hasValue shouldBe false
  }

  it should "create a class with a default java.util.Set[_] parameter" in {
    val lookup = parser(classOf[ClassWithJavaSet]).argumentLookup
    lookup.names should have size (1+numSpecialArguments)
    lookup.names should contain ("a-set")
    val argumentDefinition: ClpArgument = lookup.forArg("a-set").get
    argumentDefinition.value.get.asInstanceOf[java.util.Collection[_]].size shouldBe 0
    argumentDefinition.argumentType shouldBe classOf[java.util.Set[_]]
    argumentDefinition.unitType shouldBe classOf[Any]
    argumentDefinition.hasValue shouldBe false
  }

  it should "create a class with an Enum parameter" in {
    val lookup = parser(classOf[ClassWithEnum]).argumentLookup
    lookup.names should have size (1+numSpecialArguments)
    lookup.names should contain ("an-enum")
    val argumentDefinition: ClpArgument = lookup.forArg("an-enum").get
    argumentDefinition.value.get shouldBe LogLevel.Debug
    argumentDefinition.argumentType shouldBe classOf[LogLevel]
    argumentDefinition.hasValue shouldBe true
  }

  it should "create a class with no parameter" in {
    parser(classOf[ClassNoParams]).argumentLookup.names should have size numSpecialArguments
  }

  // These five tests are obsolete since we require defaults for all constructor parameters.
  // NB: if reflection helper sets defaults, then we can re-enable some of these tests
  /*
  it should "create a class with no default int parameter" in {
    val reflectionHelper = new ReflectionHelper(classOf[ClassWithInt])
    val task: CommandLineProgram = reflectionHelper.getDefaultInstance()
    val argumentMap = parser.createArgumentDefinitions(task)
    argumentMap should have size (1+numSpecialArguments)
    argumentMap should contain key "anInt"
    val argumentDefinition: ArgumentDefinition = argumentMap.get("anInt").get
    argumentDefinition.value.get shouldBe 0
    argumentDefinition.argumentType shouldBe classOf[Int]
    argumentDefinition.hasBeenSet shouldBe true
  }

  it should "create a class with no default string parameter" in {
    val argumentMap: Map[String, ArgumentDefinition] = parser.createArgumentDefinitions(new ClassWithString)
    argumentMap should have size (1+numSpecialArguments)
    argumentMap should contain key "aString"
    val argumentDefinition: ArgumentDefinition = argumentMap.get("aString").get
    argumentDefinition.value.get shouldBe ""
    argumentDefinition.argumentType shouldBe classOf[java.lang.String]
    argumentDefinition.hasBeenSet shouldBe true
  }

  it should "create a class with no default boolean parameter but an argument annotation" in {
    val argumentMap: Map[String, ArgumentDefinition] = parser.createArgumentDefinitions(new ClassWithBoolean)
    argumentMap should have size (1+numSpecialArguments)
    argumentMap should contain key "aBoolean"
    val argumentDefinition: ArgumentDefinition = argumentMap.get("aBoolean").get
    (argumentDefinition.value.get == null) shouldBe true
    argumentDefinition.argumentType shouldBe classOf[Boolean]
    argumentDefinition.hasBeenSet shouldBe true
  }

  it should "create a class with no default java.lang.Boolean parameter but an argument annotation" in {
    val argumentMap: Map[String, ArgumentDefinition] = parser.createArgumentDefinitions(new ClassWithJavaBoolean)
    argumentMap should have size (1+numSpecialArguments)
    argumentMap should contain key "aJavaBoolean"
    val argumentDefinition: ArgumentDefinition = argumentMap.get("aJavaBoolean").get
    (argumentDefinition.value.get == null) shouldBe true
    argumentDefinition.argumentType shouldBe classOf[java.lang.Boolean]
    argumentDefinition.hasBeenSet shouldBe true
  }

  it should "create a class with no default java.lang.Integer parameter" in {
    val argumentMap: Map[String, ArgumentDefinition] = parser.createArgumentDefinitions(new ClassWithJavaInteger)
    argumentMap should have size (1+numSpecialArguments)
    argumentMap should contain key "aJavaInteger"
    val argumentDefinition: ArgumentDefinition = argumentMap.get("aJavaInteger").get
    argumentDefinition.value.get shouldBe 0
    argumentDefinition.argumentType shouldBe classOf[java.lang.Integer]
    argumentDefinition.hasBeenSet shouldBe true
  }
  */

  it should "create a class with fields with no annotations but with defaults" in {
    val lookup = parser(classOf[NoArgAnnotationWithDefaults]).argumentLookup
    lookup.names should have size (2+numSpecialArguments)
    lookup.names should contain ("an-int")
    lookup.names should contain ("a-boolean")
    val argInt = lookup.forArg("an-int").get
    argInt.value.get shouldBe 2
    argInt.argumentType shouldBe classOf[Int]
    argInt.hasValue shouldBe true
    val argBoolean = lookup.forArg("a-boolean").get
    argBoolean.value.get shouldBe true
    argBoolean.argumentType shouldBe classOf[Boolean]
    argBoolean.hasValue shouldBe true
  }

  it should "fail to create a class with fields with no annotations but with no defaults" in {
    an[IllegalStateException] should be thrownBy parser(classOf[NoArgAnnotationWithoutDefaults]).argumentLookup
  }

  "CommandLineProgramParser.usage" should "print out no arguments when no arguments are present" in {
    val usage = parser(classOf[NoArguments]).usage(printCommon = false, withPreamble = true, withSpecial = false)
    usage should not include (RequiredArguments)
    usage should not include (OptionalArguments)
    usage should include (UsagePrefix)
  }

  it should "print out only optional arguments when only optional arguments are present" in {
    val usage = parser(classOf[OptionalOnlyArguments]).usage(printCommon = false, withPreamble = false, withSpecial = false)
    val reqIndex = usage.indexOf(RequiredArguments)
    reqIndex should be < 0
    usage should include (OptionalArguments)
  }

  it should "print out only required arguments when only required arguments are present and null default value" in {
    val usage = parser(classOf[RequiredOnlyArguments]).usage(printCommon = false, withPreamble = false, withSpecial = false)
    val reqIndex = usage.indexOf(RequiredArguments)
    reqIndex should be > 0
    usage.indexOf(OptionalArguments, reqIndex) should be < 0
  }

  /**
    * Validate the text emitted by a call to usage by ensuring that required arguments are
    * emitted before optional ones.  Assumes both optional and required arguments are present.
    */
  def validateRequiredOptionalUsage(task: Any, printCommon: Boolean): Unit = {
    val usage = parser(task.getClass).usage(printCommon = false, withPreamble = false, withSpecial = false)
    usage should include (RequiredArguments)
    usage should include (OptionalArguments)
    usage.indexOf(RequiredArguments) should be < usage.indexOf(OptionalArguments)
  }

  it should "print out both required arguments and optional arguments when both are present" in {
    val task = new GeneralTestingProgram
    validateRequiredOptionalUsage(task = task, printCommon = true)
    validateRequiredOptionalUsage(task = task, printCommon = false)
  }

  it should "print out an explanation of mutually exclusive arguments" in {
    val task = new MutexArguments
    val usage = parser(task.getClass).usage(printCommon = false, withPreamble = false, withSpecial = false)
    usage should include (mutexErrorHeader)
  }

  it should "not print out default values with None, empty collections, or required argument when no defaults are given" in {
    val usage = parser(classOf[NoDefaultValueStringsClass]).usage(printCommon = false, withPreamble = false, withSpecial = false)
    usage should not include "empty"
    usage should not include "Nil"
    usage should not include "None"
  }

  it should "not print out default values with None, empty collections, or required argument when empty defaults are given" in {
    val usage = parser(classOf[EmptyDefaultValueStringsClass]).usage(printCommon = false, withPreamble = false, withSpecial = false)
    usage should not include "empty"
    usage should not include "Nil"
    usage should not include "None"
  }

  it should "print out default values with Some(x), non-empty collections, a required argument with a non-None or non-empty collection default" in {
    val usage = parser(classOf[NonEmptyDefaultValueStringsClass]).usage(printCommon = false, withPreamble = false, withSpecial = false)
    "abcde".foreach { c =>
      usage should include (s"unique-value-$c")
    }
  }

  it should "put the clp description on the next line for a really long argument name" in {
    val usage = parser(classOf[CommandLineProgramReallyLongArg]).usage(printCommon = false, withPreamble = false, withSpecial = false)
    usage
      .split("\n")
      .filter(str => str.contains("argument"))
      .foreach(str => str should endWith ("tttttttttt=String"))
  }

  it should "pad the clp description on the same line for a short argument name" in {
    val usage = parser(classOf[CommandLineProgramShortArg]).usage(printCommon = false, withPreamble = false, withSpecial = false)
    usage
      .split("\n")
      .filter(str => str.contains("argument"))
      .foreach(str => str.endsWith("argument=String") shouldBe false)
  }

  it should "print enum values in the  usage" in {
    val usage = parser(classOf[GoodEnumClass]).usage(printCommon = false, withPreamble = false, withSpecial = false)
    GoodEnum.values.foreach(e =>
      usage should include (e.name())
    )
  }

  it should "throw an exception when an enum has no values" in {
    an[ReflectionException] should be thrownBy parser(classOf[BadEnumClass]).usage(printCommon = false, withPreamble = false, withSpecial = false)
  }

  it should "format a long description" in {
    val usage = parser(classOf[SomeDescription]).usage(printCommon = false, withPreamble = true, withSpecial = false)
    val start = usage.indexOf("<START>")
    val end = usage.indexOf("<END>")
    start should be > 0
    end should be > 0
    usage.substring(start, end).split("\n").size shouldBe 4
  }

  "CommandLineProgramParser.parseAndBuild" should "parse multiple positional arguments" in {
    val args = Array[String](
      "--string-set", "Foo", "Bar",
      "--int-set", "1", "2",
      "--int-arg", "1",
      "--string-list", "Foo", "Bar"
    )
    val p = parser(classOf[GeneralTestingProgram])
    inside (p.parseAndBuild(args=args)) { case ParseSuccess() => }
    val task = p.instance.get
    task.stringList should have size 2
    task.stringList shouldBe List[String]("Foo", "Bar")
    task.stringSet should have size 2
    task.stringSet shouldBe Set[String]("Foo", "Bar")
    task.intSet should have size 2
    task.intSet shouldBe Set[Int](1, 2)
    task.intArg shouldBe 1
    task.flag shouldBe false
    (task.enumArg == LogLevel.Debug) shouldBe true
  }

  it should "parse a flag argument" in {
    val args = Array[String](
      "--string-set", "Foo", "Bar",
      "--int-set", "1", "2",
      "-i", "1",
      "--string-list", "Foo", "Bar"
    )
    val p = parser(classOf[GeneralTestingProgram])
    inside (p.parseAndBuild(args=args)) { case ParseSuccess() => }
    val task = p.instance.get
    task.stringList should have size 2
    task.stringList shouldBe List[String]("Foo", "Bar")
    task.stringSet should have size 2
    task.stringSet shouldBe Set[String]("Foo", "Bar")
    task.intSet should have size 2
    task.intSet shouldBe Set[Int](1, 2)
    task.intArg shouldBe 1
    task.flag shouldBe false
    (task.enumArg == LogLevel.Debug) shouldBe true
  }

  it should "parse an argument that has a space" in {
    val args = Array[String](
      "--string-set", "Foo Foo", "Bar Bar",
      "--int-set", "1", "2",
      "-i", "1",
      "--string-list", "Foo", "Bar"
    )
    val p = parser(classOf[GeneralTestingProgram])
    inside (p.parseAndBuild(args=args)) { case ParseSuccess() => }
    val task = p.instance.get
    task.stringList should have size 2
    task.stringList shouldBe List[String]("Foo", "Bar")
    task.stringSet should have size 2
    task.stringSet shouldBe Set[String]("Foo Foo", "Bar Bar")
    task.intSet should have size 2
    task.intSet shouldBe Set[Int](1, 2)
    task.intArg shouldBe 1
    task.flag shouldBe false
    (task.enumArg == LogLevel.Debug) shouldBe true
  }

  it should "fail if too few positional arguments are given" in {
    val args = Array[String](
      "--string-set", "Foo",
      "--int-set", "1", "2",
      "-i", "1",
      "--string-list", "Foo", "Bar"
    )
    val p = parser(classOf[GeneralTestingProgram])
    val parseResult = p.parseAndBuild(args=args)
    inside (parseResult) { case ParseFailure(ex, remaining) =>
        ex.getMessage should include (classOf[UserException].getSimpleName)
    }
  }

  it should "fail if too many positional arguments are given" in {
    val args = Array[String](
      "--string-set", "Foo", "Bar", "Fum",
      "--int-set", "1", "2",
      "-i", "1",
      "--string-list", "Foo", "Bar"
    )
    val p = parser(classOf[GeneralTestingProgram])
    val parseResult = p.parseAndBuild(args=args)
    inside (parseResult) { case ParseFailure(ex, remaining) =>
      ex.getMessage should include (classOf[UserException].getSimpleName)
    }
  }

  it should "fail when there is a missing required argument" in {
    val args = Array[String](
      "--string-set", "Foo", "Bar",
      "--int-set", "1", "2",
      "-i", "1")
    val p = parser(classOf[GeneralTestingProgram])
    val parseResult = p.parseAndBuild(args=args)
    inside (parseResult) { case ParseFailure(ex, remaining) =>
      ex.getMessage should include (classOf[MissingArgumentException].getSimpleName)
    }
  }

  it should "fail when there is a missing required collection argument" in {
    val args = Array[String]()
    val p = parser(classOf[CollectionRequired])
    val parseResult = p.parseAndBuild(args=args)
    inside (parseResult) { case ParseFailure(ex, remaining) =>
      ex.getMessage should include (classOf[MissingArgumentException].getSimpleName)
    }
  }

  it should "fail when there is a bad argument value" in {
    val args = Array[String]("--ints", "Foo")
    val p = parser(classOf[CollectionRequired])
    val parseResult = p.parseAndBuild(args=args)
    inside (parseResult) { case ParseFailure(ex, remaining) =>
      ex.getClass should be (classOf[BadArgumentValue])
    }
  }

  it should "fail when there is a bad argument enum value" in {
    val args = Array[String]("--verbosity", "Foo")
    val p = parser(classOf[LogLevelEnumProgram])
    val parseResult = p.parseAndBuild(args=args)
    inside (parseResult) { case ParseFailure(ex, remaining) =>
      ex.getClass should be (classOf[BadArgumentValue])
    }
  }

  it should "fail when there multiple position argument values are given to a non-collection argument" in {
    val args = Array[String]("--verbosity", "Foo", "Bar")
    val p = parser(classOf[LogLevelEnumProgram])
    val parseResult = p.parseAndBuild(args=args)
    inside (parseResult) { case ParseFailure(ex, remaining) =>
      ex.getMessage should include ("verbosity")
    }
  }

  it should "fail when a non-collection argument is specified more than once" in {
    val args = Array[String]("--verbosity", "Foo", "--verbosity", "Bar")
    val p = parser(classOf[LogLevelEnumProgram])
    val parseResult = p.parseAndBuild(args=args)
    inside (parseResult) { case ParseFailure(ex, remaining) =>
      ex.getMessage should include ("verbosity")
    }
  }

  it should "accept mutex arguments" in {
    val args = Array[String]("-a", "1", "-b", "2")
    val p = parser(classOf[MutexArguments])
    inside (p.parseAndBuild(args=args)) { case ParseSuccess() => }
    val task = p.instance.get
    task.A shouldBe "1"
    task.B shouldBe "2"
  }

  def doFailingMutextTest(args: Array[String]): Unit = {
    val parseResult = parser(classOf[MutexArguments]).parseAndBuild(args=args)
    inside (parseResult) { case ParseFailure(ex, remaining) => }
  }

  it should "fail when specifying no arguments with arguments that are mutually exclusive" in {
    doFailingMutextTest(Array[String]())
  }

  it should "fail when specifying only one argument with arguments that are mutually exclusive" in {
    doFailingMutextTest(Array[String]("-A", "1"))
  }

  it should "fail when specifying arguments that are mutually exclusive" in {
    doFailingMutextTest(Array[String]("-A", "1", "-Y", "3"))
  }

  it should "fail when specifying multiple arguments with arguments that are mutually exclusive" in {
    doFailingMutextTest(Array[String]("-A", "1", "-B", "2", "-Y", "3", "-Z", "1", "-M", "2", "-N", "3"))
  }

  it should "throw a BadAnnotationException when an argument named in a mutex cannot be found" in {
    an[BadAnnotationException] should be thrownBy parser(classOf[MissingMutexArguments])
  }

  it should "accept arguments with uninitialized collections" in {
    val args = Array[String](
      "--seq", "A",
      "--set", "B",
      "--collection", "C"
    )
    val p = parser(classOf[UninitializedCollectionArguments])
    inside (p.parseAndBuild(args=args)) { case ParseSuccess() => }
    val task = p.instance.get
    task.seq shouldBe Seq("A")
    task.set shouldBe Set("B")
    task.collection shouldBe new java.util.ArrayList[Any]{ add("C".asInstanceOf[Any]) }.asInstanceOf[java.util.Collection[_]]
  }

  it should "throw a CommandLineException with a Map[String,String] argument type" in {
    val args = Array[String](
      "--map", "a b"
    )
    an[CommandLineException] should be thrownBy parser(classOf[MapCollectionArgument]).parseAndBuild(args=args)
  }

  it should "accept a collection with default values" in {
    val args = Array[String]()
    val p = parser(classOf[CollectionWithDefaults])
    inside (p.parseAndBuild(args=args)) { case ParseSuccess() => }
    val task = p.instance.get
    task.seq shouldBe Seq[String]("A", "B", "C")
  }

  it should "clear a collection with default values and add new values" in {
    val args = Array[String]("--seq", "new_value")
    val p = parser(classOf[CollectionWithDefaults])
    inside (p.parseAndBuild(args=args)) { case ParseSuccess() => }
    val task = p.instance.get
    task.seq shouldBe Seq[String]("new_value")
  }

  it should "replace defaults in a collection" in {
    val args = Array[String]("--seq", "D", "E")
    val p = parser(classOf[CollectionWithDefaults])
    inside (p.parseAndBuild(args=args)) { case ParseSuccess() => }
    val task = p.instance.get
    task.seq shouldBe Seq[String]("D", "E")
  }

  it should "accept a flag with no argument" in {
    val p = parser(classOf[FlagClass])
    p.parseAndBuild(args = Array("--flag1")) shouldBe ParseSuccess()
    val task = p.instance.get
    task.flag1 shouldBe true
  }

  it should "accept flags with with arguments" in {
    val args = Array[String]("--flag1", "true", "--flag2", "false")
    val p = parser(classOf[FlagClass])
    inside (p.parseAndBuild(args=args)) { case ParseSuccess() => }
    val task = p.instance.get
    task.flag1 shouldBe true
    task.flag2 shouldBe false
  }

  it should "accept flags with with short-form arguments" in {
    val args = Array[String]("--flag1", "T", "--flag2", "F")
    val p = parser(classOf[FlagClass])
    inside (p.parseAndBuild(args=args)) { case ParseSuccess() => }
    val task = p.instance.get
    task.flag1 shouldBe true
    task.flag2 shouldBe false
  }

  it should "accept flags with a combination of arguments and no arguments" in {
    val args = Array[String]("--flag1", "T", "--flag2")
    val p = parser(classOf[FlagClass])
    inside (p.parseAndBuild(args=args)) { case ParseSuccess() => }
    val task = p.instance.get
    task.flag1 shouldBe true
    task.flag2 shouldBe true
  }

  it should "accept setting private arguments" in {
    val args = Array[String]("--private-flag", "T", "--private-seq", "1", "2", "3", "4", "5")
    val p = parser(classOf[PrivateArguments])
    inside (p.parseAndBuild(args=args)) { case ParseSuccess() => }
    val task = p.instance.get
    task.getPrivateFlag shouldBe true
    task.getPrivateSeq shouldBe Seq("1", "2", "3", "4", "5")
  }

  it should s"accept the help special flag (--help)" in {
    // try just with the flag
    var args = Array[String]("--help")
    parser(classOf[ClassNoParams]).parseAndBuild(args=args) shouldBe ParseHelp()

    // try with the flag and true arg
    args = Array[String]("--help", "true")
    parser(classOf[ClassNoParams]).parseAndBuild(args=args) shouldBe ParseHelp()

    // try with the flag and false arg
    args = Array[String]("--help", "false")
    parser(classOf[ClassNoParams]).parseAndBuild(args=args) shouldBe ParseSuccess()
  }

  it should s"accept the version special flag (--version)" in {
    val task = new ClassNoParams

    // try just with the flag
    var args = Array[String]("--version")
    parser(classOf[ClassNoParams]).parseAndBuild(args=args) shouldBe ParseVersion()

    // try with the flag and true arg
    args = Array[String]("--version", "true")
    parser(classOf[ClassNoParams]).parseAndBuild(args=args) shouldBe ParseVersion()

    // try with the flag and false arg
    args = Array[String]("--version", "false")
    parser(classOf[ClassNoParams]).parseAndBuild(args=args) shouldBe ParseSuccess()
  }

  it should "accept a val argument" in {
    val args = Array[String]("--flag")
    val p = parser(classOf[ValFlagClass])
    p.parseAndBuild(args=args) shouldBe ParseSuccess()
    p.instance.get.flag shouldBe true
  }

  it should "accept an argument without a val/var declaration" in {
    val args = Array[String]("--flag")
    val p = parser(classOf[NoVarFlagClass])
    p.parseAndBuild(args=args) shouldBe ParseSuccess()
    p.instance.get.flag shouldBe true
  }

  it should "accept a boolean argument with --flag=false" in {
    val args = Array[String]("--flag=true")
    val p = parser(classOf[NoVarFlagClass])
    p.parseAndBuild(args=args) shouldBe ParseSuccess()
    p.instance.get.flag shouldBe true
  }

  it should "fail when it cannot load an arguments file" in {
    val args = Array[String]("@/path/to/nowhere")
    val p = parser(classOf[ClassWithInt])
    val parseResult = p.parseAndBuild(args = args)
    inside(parseResult) { case ParseFailure(ex, remaining) =>
      ex.getMessage should include(CommandLineProgramParserStrings.CannotLoadArgumentFilesMessage)
    }
  }

  it should "throw a UserException when mutually exclusive arguments are used" in {
    val task = new MutexArguments
    val args = Array[String]("-a", "a", "-m", "m", "-n", "n", "-y", "y", "-z", "z")
    val p = parser(task.getClass)
    val parseResult = p.parseAndBuild(args=args)
    inside (parseResult) { case ParseFailure(ex, remaining) =>
      ex.getMessage should include (classOf[UserException].getSimpleName)
    }
  }

  "CommandLineProgramParser.getCommandLine" should "should return the command line string" in {
    val task = new GeneralTestingProgram
    val args = Array[String](
      "--string-set", "Foo", "Bar",
      "--int-set", "1", "2",
      "-i", "1",
      "--string-list", "Foo", "Bar"
    )

    val p = parser(classOf[GeneralTestingProgram])
    inside (p.parseAndBuild(args=args)) { case ParseSuccess() => }
    val commandLine: String = p.commandLine()
    commandLine shouldBe "GeneralTestingProgram --string-set Foo Bar --int-set 1 2 --int-arg 1 --enum-arg Debug --string-list Foo Bar --flag false"
  }

  it should "should return the command line string but not show a sensitive arg" in {
    val task = new SensitiveArgTestingProgram
    val args = Array[String]()
    val p = parser(classOf[SensitiveArgTestingProgram])
    inside (p.parseAndBuild(args=args)) { case ParseSuccess() => }
    val commandLine: String = p.commandLine()
    commandLine shouldBe s"${task.getClass.getSimpleName} --flag ***********"
  }

  "CommandLineProgramParser.version" should "return the version" in {
    parser(classOf[SensitiveArgTestingProgram]).version should include ("Version")
  }

  "CommandLineProgramParser.parseAndBuild" should "return None if -h is given" in {
    val stderr: String = captureStderr(() => {
      CommandLineProgramParser.parseAndBuild(targetClass = classOf[ClassWithInt], args = Array[String]("--an-int", "1", "-h")) shouldBe 'empty
    })
    stderr should include (UsagePrefix)
  }

  it should "return None if -v is given" in {
    val stderr: String = captureStderr(() => {
      CommandLineProgramParser.parseAndBuild(targetClass = classOf[ClassWithInt], args = Array[String]("--an-int", "1", "-v")) shouldBe 'empty
    })
    stderr should include ("Version")
  }

  it should "return None if illegal arguments are given" in {
    val stderr: String = captureStderr(() => {
      CommandLineProgramParser.parseAndBuild(targetClass = classOf[ClassWithInt], args = Array[String]("--illegal-option")) shouldBe 'empty
    })
    stderr.nonEmpty shouldBe true
  }

  it should "return an instance with arguments if parsing was successful" in {
    val stderr: String = captureStderr(() => {
      val instanceOption = CommandLineProgramParser.parseAndBuild(targetClass = classOf[ClassWithInt], args = Array[String]("--an-int", "1"))
      instanceOption shouldBe 'defined
      instanceOption.foreach(instance => instance.anInt shouldBe 1)
    })
    stderr shouldBe 'empty
  }
}
