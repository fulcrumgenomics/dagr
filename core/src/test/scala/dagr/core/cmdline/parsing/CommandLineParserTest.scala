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
package dagr.core.cmdline.parsing

import dagr.core.cmdline._
import dagr.core.cmdline.parsing.CommandLineParserStrings._
import dagr.core.tasksystem.Task
import dagr.core.util.{LogLevel, UnitSpec}
import org.scalatest.OptionValues

import scala.collection.Map
import scala.collection.immutable.HashMap
import scala.language.existentials

////////////////////////////////////////////////////////////////////////////////
// Holds the classes we need for testing.  These cannot be inner classes.
////////////////////////////////////////////////////////////////////////////////

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private class CommandLineTaskTesting extends Task {
  override def getTasks: Traversable[_ <: Task] = Nil
}

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassNoParams @CLPConstructor
() extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithInt @CLPConstructor
(@Arg var anInt: Int) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithIntDefault @CLPConstructor
(@Arg var anInt: Int = 2) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithString @CLPConstructor
(@Arg var aString: String) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithStringDefault @CLPConstructor
(@Arg var aString: String = "string") extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithNullString @CLPConstructor
(@Arg var aString: String = null) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithBoolean @CLPConstructor
(@Arg var aBoolean: Boolean) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithBooleanDefault @CLPConstructor
(@Arg var aBoolean: Boolean = true) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithJavaInteger @CLPConstructor
(@Arg var aJavaInteger: java.lang.Integer) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithJavaIntegerWithDefault @CLPConstructor
(@Arg var aJavaInteger: java.lang.Integer = 2) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithJavaBoolean @CLPConstructor
(@Arg var aJavaBoolean: java.lang.Boolean) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithJavaBooleanWithDefault @CLPConstructor
(@Arg var aJavaBoolean: java.lang.Boolean = true) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithOptionWithSomeDefault @CLPConstructor
(@Arg var anOption: Option[_] = Some(2)) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithOptionWithNoneDefault @CLPConstructor
(@Arg var anOption: Option[_] = None) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithOptionWithNoneDefaultAndOptional @CLPConstructor
(@Arg var anOption: Option[_] = None) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithIntOptionWithSomeDefault @CLPConstructor
(@Arg var anOption: Option[Int] = Some(2)) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithIntOptionWithNoneDefault @CLPConstructor
(@Arg var anOption: Option[Int] = None) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithList @CLPConstructor
(@Arg var aList: List[_] = Nil) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithIntList @CLPConstructor
(@Arg var aList: List[Int] = Nil) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithSeq @CLPConstructor
(@Arg var aSeq: scala.collection.Seq[_] = Nil) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithCollection @CLPConstructor
(@Arg var aCollection: java.util.Collection[_] = new java.util.ArrayList[Any]()) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithJavaSet @CLPConstructor
(@Arg var aSet: java.util.Set[_] = new java.util.HashSet[Any]()) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ClassWithEnum @CLPConstructor
(@Arg var anEnum: LogLevel = LogLevel.Debug) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class GeneralTestingTask @CLPConstructor
(
  @Arg(minElements = 2, maxElements = 2) var stringSet: scala.collection.Set[String] = null, // also tests if we can initialize it with a set
  @Arg(minElements = 2, maxElements = 2) var intSet: scala.collection.Set[Int] = null, // also tests if we can initialize it with a set
  @Arg(flag = "i"                  )     var intArg: Int = 1, // tests short name
  @Arg                                   var enumArg: LogLevel = LogLevel.Debug, // tests enums
  @Arg                                   var stringList: List[String] = List[String](), // tests non empty??? TODO
  @Arg                                   var flag: Boolean = false
) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class NoArguments @CLPConstructor
() extends CommandLineTaskTesting


@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class OptionalOnlyArguments @CLPConstructor
(@Arg var arg: String = "42") extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class RequiredOnlyArguments @CLPConstructor
(@Arg var arg: String = null) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class CollectionRequired @CLPConstructor() (@Arg var ints: List[Int] = Nil)

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class LogLevelEnumTask @CLPConstructor() (@Arg var verbosity: LogLevel = LogLevel.Info)

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private[cmdline] case class MutexArguments @CLPConstructor
(
  @Arg(mutex = Array("M", "N", "Y", "Z")) var A: String = null,
  @Arg(mutex = Array("M", "N", "Y", "Z")) var B: String = null,
  @Arg(mutex = Array("A", "B", "Y", "Z")) var M: String = null,
  @Arg(mutex = Array("A", "B", "Y", "Z")) var N: String = null,
  @Arg(mutex = Array("A", "B", "M", "N")) var Y: String = null,
  @Arg(mutex = Array("A", "B", "M", "N")) var Z: String = null
)

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private[cmdline] case class MissingMutexArguments @CLPConstructor
(
  @Arg(mutex = Array("B")) var A: String = null,
  @Arg(mutex = Array("C")) var B: String = null // C should not be found
)

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class MapCollectionArgument @CLPConstructor
(
  @Arg var map: Map[String,String] = new HashMap[String,String]()
)

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class CollectionWithDefaults @CLPConstructor
(
  @Arg var seq: Seq[String] = Seq[String]("A", "B", "C")
)

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class FlagClass @CLPConstructor
(
  @Arg var flag1: Boolean = false,
  @Arg var flag2: Boolean = true
)

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class PrivateArguments @CLPConstructor
(
  @Arg private var privateFlag: Boolean = false,
  @Arg private var privateSeq: Seq[_] = Nil

) {
  def getPrivateFlag: Boolean = privateFlag
  def getPrivateSeq: Seq[_] = privateSeq
}

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class ValFlagClass @CLPConstructor
(
  @Arg flag: Boolean = false
) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class NoVarFlagClass @CLPConstructor
(
  @Arg flag: Boolean = false
) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class SensitiveArgTestingTask @CLPConstructor
(
  @Arg(sensitive = true) var flag: Boolean = false
) extends CommandLineTaskTesting

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class UninitializedCollectionArguments @CLPConstructor
(
  @Arg var seq: Seq[_] = Nil,
  @Arg var set: Set[_] = Set.empty,
  @Arg var collection: java.util.Collection[_] = null
)

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class NoArgAnnotationWithDefaults @CLPConstructor
(
  var anInt: Int = 2,
  var aBoolean: Boolean = true
)

@CLP(description = "", group = classOf[TestGroup], hidden = true)
private case class NoArgAnnotationWithoutDefaults @CLPConstructor
(
  var anInt: Int,
  var aBoolean: Boolean
)

////////////////////////////////////////////////////////////////////////////////
// End of Testing CLP classes
////////////////////////////////////////////////////////////////////////////////

class CommandLineParserTest extends UnitSpec with OptionValues {
  // Create a new parser for each test
  def parser[T](clazz: Class[T]) : CommandLineParser[T] = new CommandLineParser(clazz)

  // How many special argument names are there? Will need to be changed if SpecialArgumentsCollection changes
  private val numSpecialArguments: Int = 3

  "CommandLineParser.createArgumentDefinition" should "create a class with a default int parameter" in {
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
    val task: CommandLineTask = reflectionHelper.getDefaultInstance()
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

  "CommandLineParser.usage" should "print out no arguments when no arguments are present" in {
    val usage = parser(classOf[NoArguments]).usage(printCommon = false, withPreamble = false, withSpecial = false)
    usage.indexOf(RequiredArguments) should be < 0
    usage.indexOf(OptionalArguments) should be < 0
  }

  it should "print out only optional arguments when only optional arguments are present" in {
    val usage = parser(classOf[OptionalOnlyArguments]).usage(printCommon = false, withPreamble = false, withSpecial = false)
    val reqIndex = usage.indexOf(RequiredArguments)
    reqIndex should be < 0
    usage.indexOf(OptionalArguments, reqIndex) should be > 0
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
    val reqIndex = usage.indexOf(RequiredArguments)
    reqIndex should be > 0
    usage.indexOf(OptionalArguments, reqIndex) should be > 0
  }

  it should "print out both required arguments and optional arguments when both are present" in {
    val task = new GeneralTestingTask
    validateRequiredOptionalUsage(task = task, printCommon = true)
    validateRequiredOptionalUsage(task = task, printCommon = false)
  }

  "CommandLineParser.parseTaskArgs" should "parse multiple positional arguments" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String](
      "--string-set", "Foo", "Bar",
      "--int-set", "1", "2",
      "--int-arg", "1",
      "--string-list", "Foo", "Bar"
    )
    val p = parser(classOf[GeneralTestingTask])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
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
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String](
      "--string-set", "Foo", "Bar",
      "--int-set", "1", "2",
      "-i", "1",
      "--string-list", "Foo", "Bar"
    )
    val p = parser(classOf[GeneralTestingTask])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
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
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String](
      "--string-set", "Foo Foo", "Bar Bar",
      "--int-set", "1", "2",
      "-i", "1",
      "--string-list", "Foo", "Bar"
    )
    val p = parser(classOf[GeneralTestingTask])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
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
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String](
      "--string-set", "Foo",
      "--int-set", "1", "2",
      "-i", "1",
      "--string-list", "Foo", "Bar"
    )
    val p = parser(classOf[GeneralTestingTask])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Failure
    errorMessageBuilder.isEmpty shouldBe false
    errorMessageBuilder.toString.indexOf(classOf[UserException].getSimpleName) should be > 0
  }

  it should "fail if too many positional arguments are given" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String](
      "--string-set", "Foo", "Bar", "Fum",
      "--int-set", "1", "2",
      "-i", "1",
      "--string-list", "Foo", "Bar"
    )
    val p = parser(classOf[GeneralTestingTask])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Failure
    errorMessageBuilder.isEmpty shouldBe false
    errorMessageBuilder.toString.indexOf(classOf[UserException].getSimpleName) should be > 0
  }

  it should "fail when there is a missing required argument" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String](
      "--string-set", "Foo", "Bar",
      "--int-set", "1", "2",
      "-i", "1")
    val p = parser(classOf[GeneralTestingTask])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Failure
    errorMessageBuilder.isEmpty shouldBe false
    errorMessageBuilder.toString.indexOf(classOf[MissingArgumentException].getSimpleName) should be > 0
  }

  it should "fail when there is a missing required collection argument" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]()
    val p = parser(classOf[CollectionRequired])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Failure
    errorMessageBuilder.isEmpty shouldBe false
    errorMessageBuilder.toString.indexOf(classOf[MissingArgumentException].getSimpleName) should be > 0
  }

  it should "fail when there is a bad argument value" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]("--ints", "Foo")
    val p = parser(classOf[CollectionRequired])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Failure
    errorMessageBuilder.isEmpty shouldBe false
    errorMessageBuilder.toString.indexOf(classOf[BadArgumentValue].getSimpleName) should be > 0
  }

  it should "fail when there is a bad argument enum value" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]("--verbosity", "Foo")
    val p = parser(classOf[LogLevelEnumTask])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Failure
    errorMessageBuilder.isEmpty shouldBe false
    errorMessageBuilder.toString.indexOf(classOf[BadArgumentValue].getSimpleName) should be > 0
  }

  it should "fail when there multiple position argument values are given to a non-collection argument" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]("--verbosity", "Foo", "Bar")
    val p = parser(classOf[LogLevelEnumTask])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Failure
    errorMessageBuilder.isEmpty shouldBe false
    errorMessageBuilder.toString().indexOf("verbosity") should be > 0
  }

  it should "fail when a non-collection argument is specified more than once" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]("--verbosity", "Foo", "--verbosity", "Bar")
    val p = parser(classOf[LogLevelEnumTask])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Failure
    errorMessageBuilder.isEmpty shouldBe false
    errorMessageBuilder.toString().indexOf("verbosity") should be > 0
  }

  it should "accept mutex arguments" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]("-a", "1", "-b", "2")
    val p = parser(classOf[MutexArguments])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
    errorMessageBuilder.isEmpty shouldBe true
    val task = p.instance.get    
    task.A shouldBe "1"
    task.B shouldBe "2"
  }

  def doFailingMutextTest(args: Array[String]): Unit = {
    val task = new MutexArguments
    val errorMessageBuilder: StringBuilder = new StringBuilder
    parser(classOf[MutexArguments]).parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Failure
    errorMessageBuilder.nonEmpty shouldBe true
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
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String](
      "--seq", "A",
      "--set", "B",
      "--collection", "C"
    )
    val p = parser(classOf[UninitializedCollectionArguments])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
    val task = p.instance.get    
    task.seq shouldBe Seq("A")
    task.set shouldBe Set("B")
    task.collection shouldBe new java.util.ArrayList[Any]() { add("C".asInstanceOf[Any]) }.asInstanceOf[java.util.Collection[_]]
  }

  it should "throw a CommandLineException with a Map[String,String] argument type" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String](
      "--map", "a b"
    )
    an[CommandLineException] should be thrownBy parser(classOf[MapCollectionArgument]).parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args)
  }

  it should "accept a collection with default values" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]()
    val p = parser(classOf[CollectionWithDefaults])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
    errorMessageBuilder.isEmpty shouldBe true
    val task = p.instance.get    
    task.seq shouldBe Seq[String]("A", "B", "C")
  }

  it should "clear a collection with default values and add new values" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]("--seq", "new_value")
    val p = parser(classOf[CollectionWithDefaults])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
    errorMessageBuilder.isEmpty shouldBe true
    val task = p.instance.get
    task.seq shouldBe Seq[String]("new_value")
  }

  it should "replace defaults in a collection" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]("--seq", "D", "E")
    val p = parser(classOf[CollectionWithDefaults])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
    errorMessageBuilder.isEmpty shouldBe true
    val task = p.instance.get
    task.seq shouldBe Seq[String]("D", "E")
  }

  it should "accept a flag with no argument" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val p = parser(classOf[FlagClass])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = Array("--flag1")) shouldBe ParseResult.Success
    errorMessageBuilder.isEmpty shouldBe true
    val task = p.instance.get
    task.flag1 shouldBe true
  }

  it should "accept flags with with arguments" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]("--flag1", "true", "--flag2", "false")
    val p = parser(classOf[FlagClass])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
    errorMessageBuilder.isEmpty shouldBe true
    val task = p.instance.get
    task.flag1 shouldBe true
    task.flag2 shouldBe false
  }

  it should "accept flags with with short-form arguments" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]("--flag1", "T", "--flag2", "F")
    val p = parser(classOf[FlagClass])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
    errorMessageBuilder.isEmpty shouldBe true
    val task = p.instance.get
    task.flag1 shouldBe true
    task.flag2 shouldBe false
  }

  it should "accept flags with a combination of arguments and no arguments" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]("--flag1", "T", "--flag2")
    val p = parser(classOf[FlagClass])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
    errorMessageBuilder.isEmpty shouldBe true
    val task = p.instance.get
    task.flag1 shouldBe true
    task.flag2 shouldBe true
  }

  it should "accept setting private arguments" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]("--private-flag", "T", "--private-seq", "1", "2", "3", "4", "5")
    val p = parser(classOf[PrivateArguments])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
    errorMessageBuilder.isEmpty shouldBe true
    val task = p.instance.get
    task.getPrivateFlag shouldBe true
    task.getPrivateSeq shouldBe Seq("1", "2", "3", "4", "5")
  }

  it should s"accept the help special flag (--${SpecialArgumentsCollection.HELP_FULLNAME})" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder

    // try just with the flag
    var args = Array[String]("--" + SpecialArgumentsCollection.HELP_FULLNAME)
    parser(classOf[ClassNoParams]).parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Help
    errorMessageBuilder.isEmpty shouldBe true

    // try with the flag and true arg
    args = Array[String]("--" + SpecialArgumentsCollection.HELP_FULLNAME, "true")
    parser(classOf[ClassNoParams]).parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Help

    // try with the flag and false arg
    args = Array[String]("--" + SpecialArgumentsCollection.HELP_FULLNAME, "false")
    parser(classOf[ClassNoParams]).parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
  }

  it should s"accept the version special flag (--${SpecialArgumentsCollection.VERSION_FULLNAME})" in {
    val task = new ClassNoParams
    val errorMessageBuilder: StringBuilder = new StringBuilder

    // try just with the flag
    var args = Array[String]("--" + SpecialArgumentsCollection.VERSION_FULLNAME)
    parser(classOf[ClassNoParams]).parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Version
    errorMessageBuilder.isEmpty shouldBe true

    // try with the flag and true arg
    args = Array[String]("--" + SpecialArgumentsCollection.VERSION_FULLNAME, "true")
    parser(classOf[ClassNoParams]).parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Version

    // try with the flag and false arg
    args = Array[String]("--" + SpecialArgumentsCollection.VERSION_FULLNAME, "false")
    parser(classOf[ClassNoParams]).parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
  }

  it should "accept a val argument" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]("--flag")
    val p = parser(classOf[ValFlagClass])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
    errorMessageBuilder.isEmpty shouldBe true
    p.instance.get.flag shouldBe true
  }

  it should "accept an argument without a val/var declaration" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]("--flag")
    val p = parser(classOf[NoVarFlagClass])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
    errorMessageBuilder.isEmpty shouldBe true
    p.instance.get.flag shouldBe true
  }

  it should "accept a boolean argument with --flag=false" in {
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]("--flag=true")
    val p = parser(classOf[NoVarFlagClass])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
    errorMessageBuilder.isEmpty shouldBe true
    p.instance.get.flag shouldBe true
  }

  "CommandLineParser.getCommandLine" should "should return the command line string" in {
    val task = new GeneralTestingTask
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String](
      "--string-set", "Foo", "Bar",
      "--int-set", "1", "2",
      "-i", "1",
      "--string-list", "Foo", "Bar"
    )

    val p = parser(classOf[GeneralTestingTask])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
    val commandLine: String = p.commandLine()
    commandLine shouldBe "GeneralTestingTask --string-set Foo Bar --int-set 1 2 --int-arg 1 --enum-arg Debug --string-list Foo Bar --flag false"
  }

  it should "should return the command line string but not show a sensitive arg" in {
    val task = new SensitiveArgTestingTask
    val errorMessageBuilder: StringBuilder = new StringBuilder
    val args = Array[String]()
    val p = parser(classOf[SensitiveArgTestingTask])
    p.parseAndBuild(errorMessageBuilder = errorMessageBuilder, args = args) shouldBe ParseResult.Success
    val commandLine: String = p.commandLine()
    commandLine shouldBe s"${task.getClass.getSimpleName} --flag ***********"
  }
}
