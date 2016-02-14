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

import dagr.commons.reflect.ReflectionUtil
import dagr.commons.util.{CaptureSystemStreams, Logger, UnitSpec, LogLevel}
import dagr.sopt._
import dagr.sopt.cmdline.testing.clps._
import dagr.sopt.util.TermCode
import org.scalatest.BeforeAndAfterAll

import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

// Located here since we cannot be an inner class
@clp(description = "", group = classOf[TestGroup], hidden = true)
class CommandLineProgramValidionError(@arg var aStringSomething: String = "Default") {
  throw new ValidationException("WTF")
}

class CommandLineParserTest extends UnitSpec with CaptureSystemStreams with BeforeAndAfterAll {

  private val prevPrintColor = TermCode.printColor
  override protected def beforeAll(): Unit = TermCode.printColor = false
  override protected def afterAll(): Unit = TermCode.printColor = prevPrintColor

  private def nameOf(clazz: Class[_]): String = clazz.getSimpleName

  private object TestParseSubCommand {
    val packageList: List[String] = List[String]("dagr.sopt.cmdline.testing.clps")

    def parseSubCommand[ClpClass](args: Array[String],
                   clpBlock: Option[ClpClass => Unit] = None,
                   extraUsage: Option[String] = None
                  )
                  (implicit classTag: ClassTag[ClpClass], typeTag: TypeTag[ClpClass]): (CommandLineParser[ClpClass], Option[ClpClass], String)
     = {
      val parser = new CommandLineParser[ClpClass]("command-line-name")
      Logger.synchronized { // since we modify the log level
      val logLevel = Logger.level
        Logger.level = LogLevel.Fatal // turn off all logging
        var clpOption: Option[ClpClass] = None
        val output: String = captureStderr(() => {
          def blockMethod: ClpClass => Unit = clpBlock.getOrElse((t: ClpClass) => Unit)
          parser.parseSubCommand(
            args=args,
            packageList=packageList,
            includeHidden = true,
            extraUsage = extraUsage,
            afterSubCommandBuild = blockMethod) match {
            case Some(clp) =>
              clpOption = Some(clp)
            case None => Unit
          }
        })
        Logger.level = logLevel // turn logging back on
        (parser, clpOption, output)
      }
    }

    def checkEmptyUsage[T](parser:  CommandLineParser[T], clpOption: Option[T], output: String): Unit = {
      clpOption shouldBe 'empty
      output should include(parser.AvailableSubCommands)
      output should not include parser.unknownSubCommandErrorMessage("")
    }

    def checkEmptyClpUsage[T](parser:  CommandLineParser[T], clpOption: Option[T], output: String, clpClazz: Class[_]): Unit = {
      val name = nameOf(clpClazz)
      clpOption shouldBe 'empty
      output should include(parser.standardSubCommandUsagePreamble(Some(clpClazz)))
      output should include(s"$name ${parser.RequiredArguments}")
      output should include(s"$name ${parser.OptionalArguments}")
    }
  }

  // required so that colors are not in our usage messages
  "CommandLineParserTest" should "have color status be false" in {
    // this should be set in the build.sbt
    TermCode.printColor shouldBe false
  }

  "CommandLineParser.parseSubCommand" should "print just the available Clps and a missing Clp error when no arguments or the help flag are/is given" in {
    Stream(Array[String](), Array[String]("-h"), Array[String]("--help")).foreach { args =>
      val (parser, clpOption, output) = TestParseSubCommand.parseSubCommand[TestingClp](args)
      TestParseSubCommand.checkEmptyUsage(parser, clpOption, output)
      if (args.isEmpty) {
        output should include(parser.AvailableSubCommands)
        output should include(parser.MissingSubCommand)
      }
    }
  }

  it should "print just the usage when an unknown Clp name is given" in {
    Stream(
      Array[String]("--", "ClpFive"),
      Array[String]("--", "ClpFive", "--flag"),
      Array[String]("ClpFive", "--flag")
    ).foreach { args =>
      val (parser, clpOption, output) = TestParseSubCommand.parseSubCommand[TestingClp](args)
      clpOption shouldBe 'empty
      TestParseSubCommand.checkEmptyUsage(parser, clpOption, output)
    }
  }

  it should "print just the usage when the Clp name then no arguments, -h, or --help is given" in {
    val name = nameOf(classOf[CommandLineProgramThree])
    Stream(Array[String](name), Array[String](name, "-h"), Array[String](name, "--help")).foreach { args =>
      val (parser, clpOption, output) = TestParseSubCommand.parseSubCommand[TestingClp](args)
      TestParseSubCommand.checkEmptyClpUsage(parser, clpOption, output, classOf[CommandLineProgramThree])
    }
  }

  it should "print just the command version when the Clp name then -v or --version is given" in {
    val name = nameOf(classOf[CommandLineProgramThree])
    Stream(Array[String](name, "-v"), Array[String](name, "--version")).foreach { args =>
      val (parser, _, output) = TestParseSubCommand.parseSubCommand[TestingClp](args)
      val version = new CommandLineProgramParser(classOf[TestingClp]).version
      output should include(version)
    }
  }

  it should "print just the usage when unknown arguments are passed after a valid Clp name" in {
    val clpClazz = classOf[CommandLineProgramFour]
    val name = nameOf(clpClazz)
    Stream(
      Array[String](name, "--helloWorld", "SomeProgram"),
      Array[String](name, "---", "SomeProgram")
    ).foreach { args =>
      val (parser, clpOption, output) = TestParseSubCommand.parseSubCommand[TestingClp](args)
      clpOption shouldBe 'empty
      output should include(parser.standardSubCommandUsagePreamble(Some(clpClazz)))
      output should include(s"No option found with name '${args(1).substring(2)}'")
    }
  }

  it should "list all command names when multiple have the same prefix of the given command argument" in {
    val (parser, clpOption, output) = TestParseSubCommand.parseSubCommand[TestingClp](Array[String](" CommandLineProgram"))
    output should include(nameOf(classOf[CommandLineProgramOne]))
    output should include(nameOf(classOf[CommandLineProgramTwo]))
    output should include(nameOf(classOf[CommandLineProgramThree]))
    output should include(nameOf(classOf[CommandLineProgramFour]))
    // ... should match them all!
  }

  it should "return a valid clp with and valid arguments" in {
    val clpClazz = classOf[CommandLineProgramThree]
    val name = nameOf(clpClazz)
    Stream(
      Array[String](name, "--argument", "value")
    ).foreach { args =>
      val (parser, clpOption, output) = TestParseSubCommand.parseSubCommand[TestingClp](args)
      clpOption shouldBe 'defined
      clpOption.get.getClass shouldBe clpClazz
      clpOption.get.asInstanceOf[CommandLineProgramThree].argument shouldBe "value"
      output shouldBe 'empty
    }
  }

  it should "print a usage when a ValidationException is thrown the block method" in {
    val clpClazz = classOf[CommandLineProgramThree]
    val name = nameOf(clpClazz)
    val args = Array[String](name, "--argument", "value")
    val blockMethod: CommandLineProgramThree => Unit = (clp: CommandLineProgramThree) => {
      throw new ValidationException("Testing")
    }
    val (parser, clpOption, output) = TestParseSubCommand.parseSubCommand[CommandLineProgramThree](args, clpBlock=Some(blockMethod))
    output should include ("Testing")
  }

  it should "print an extra usage if desired" in {
    val clpClazz = classOf[CommandLineProgramThree]
    val name = nameOf(clpClazz)
    val args = Array[String]("-h")
    val (parser, clpOption, output) = TestParseSubCommand.parseSubCommand[CommandLineProgramThree](args, extraUsage=Some("HELLO WORLD"))
    output should include ("HELLO WORLD")
  }

  "CommandLineParser.formatShortDescription" should "print everything before the first period when present" in {
    val parser = new CommandLineParser[CommandLineParserTest](commandLineName="Name")
    parser.formatShortDescription("A.B.C.D.") shouldBe "A."
    parser.formatShortDescription("blah. Tybalt, here slain, whom Romeo's hand did slay; blah. blah.") shouldBe "blah."
  }

  it should "print at most the maximum line length" in {
    val parser = new CommandLineParser[CommandLineParserTest](commandLineName="Name")
    parser.formatShortDescription("A B C D E F G") shouldBe "A B C D E F G"
    parser.formatShortDescription("A" * (parser.MaximumLineLength+1)) shouldBe (("A" * (parser.MaximumLineLength-3)) + "...")
  }

  private object TestParsecommandAndClp {
    val packageList: List[String] = List[String]("dagr.sopt.cmdline.testing.clps")

    def parseCommandAndClp[commandClass,ClpClass](args: Array[String],
                                            commandBlock: commandClass => Unit = (t: commandClass) => (),
                                            clpBlock: commandClass => ClpClass => Unit = (t: commandClass) => (c: ClpClass) => () )
                                           (implicit classTag: ClassTag[ClpClass], typeTag: TypeTag[ClpClass], tt: TypeTag[commandClass])
    : (CommandLineParser[ClpClass], Option[commandClass], Option[ClpClass], String) = {
      val commandClazz: Class[commandClass] = ReflectionUtil.typeTagToClass[commandClass]
      val commandName = commandClazz.getSimpleName
      val parser = new CommandLineParser[ClpClass](commandName)
      Logger.synchronized { // since we modify the log level
      val logLevel = Logger.level
        Logger.level = LogLevel.Fatal // turn off all logging
        var commandOption: Option[commandClass] = None
        var clpOption: Option[ClpClass] = None
        val output: String = captureStderr(() => {
          parser.parseCommandAndSubCommand[commandClass](
            args=args,
            packageList=packageList,
            includeHidden = true,
            afterCommandBuild = commandBlock,
            afterSubCommandBuild = clpBlock) match {
            case Some((commandInstance, clpInstance)) =>
              commandOption = Some(commandInstance)
              clpOption = Some(clpInstance)
            case None => Unit
          }
        })
        Logger.level = logLevel // turn logging back on
        (parser, commandOption, clpOption, output)
      }
    }

    def checkEmptyUsage[commandClass,ClpClass](parser:  CommandLineParser[ClpClass],
                                            commandOption: Option[commandClass],
                                            clpOption: Option[ClpClass],
                                            output: String)
                                           (implicit ttCommand: TypeTag[commandClass])
    : Unit = {
      val commandClazz: Class[commandClass] = ReflectionUtil.typeTagToClass[commandClass]
      val commandName = commandClazz.getSimpleName
      commandOption shouldBe 'empty
      clpOption shouldBe 'empty
      output should include(parser.standardCommandAndSubCommandUsagePreamble(commandClazz=Some(commandClazz), subCommandClazz=None))
      output should include(s"$commandName ${parser.OptionalArguments}")
      output should include(parser.AvailableSubCommands)
    }

    def checkEmptyClpUsage[commandClass,ClpClass](parser:  CommandLineParser[ClpClass], commandOption: Option[commandClass], clpOption: Option[ClpClass], output: String)
                                              (implicit ttCommand: TypeTag[commandClass], ttClp: TypeTag[ClpClass])
    : Unit = {
      val commandClazz: Class[commandClass] = ReflectionUtil.typeTagToClass[commandClass]
      val clpClazz: Class[ClpClass] = ReflectionUtil.typeTagToClass[ClpClass]
      val commandName = nameOf(commandClazz)
      val name = nameOf(clpClazz)
      commandOption shouldBe 'empty
      clpOption shouldBe 'empty
      output should include(parser.standardCommandAndSubCommandUsagePreamble(commandClazz=Some(commandClazz), subCommandClazz=Some(clpClazz)))
      // NB: required arguments are not checked since the command class has all optional arguments
      //output should include(s"$commandName ${parser.RequiredArguments}")
      output should include(s"$commandName ${parser.OptionalArguments}")
      output should include(s"$name ${parser.RequiredArguments}")
      output should include(s"$name ${parser.OptionalArguments}")
    }
  }

  "CommandLineParser.parseCommandAndSubCommand" should "print just the command usage when no arguments or the help flag are/is given" in {
    Stream(Array[String](), Array[String]("-h"), Array[String]("--help")).foreach { args =>
      val (parser, commandOption, clpOption, output) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramTesting,CommandLineProgramOne](args)
      TestParsecommandAndClp.checkEmptyUsage[CommandLineProgramTesting,CommandLineProgramOne](parser=parser, commandOption=commandOption, clpOption=clpOption, output=output)
      if (args.isEmpty) {
        output should include(parser.MissingSubCommand)
      }
    }
  }

  it should "print just the command version when -v or --version is given" in {
    Stream(Array[String]("-v"), Array[String]("--version")).foreach { args =>
      val (parser, commandOption, clpOption, output) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramTesting,CommandLineProgramOne](args)
      val version: String = new CommandLineProgramParser(classOf[CommandLineProgramTesting]).version
      output should include(version)
    }
  }

  it should "print just the command usage when only command and clp separator \"--\" is given" in {
    val args = Array[String]("--")
    val (parser, commandOption, clpOption, output) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramTesting,CommandLineProgramOne](args)
    TestParsecommandAndClp.checkEmptyUsage[CommandLineProgramTesting,CommandLineProgramOne](parser=parser, commandOption=commandOption, clpOption=clpOption, output=output)
    output should include(parser.MissingSubCommand)
  }

  it should "print just the command usage when unknown arguments are passed to command" in {
    Stream(
      Array[String]("--helloWorld", "CommandLineProgramTesting"),
      Array[String]("---", "CommandLineProgramTesting")
    ).foreach { args =>
      val (parser, commandOption, clpOption, output) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramTesting,CommandLineProgramOne](args)
      commandOption shouldBe 'empty
      clpOption shouldBe 'empty
      output should include(parser.standardCommandAndSubCommandUsagePreamble(commandClazz=Some(classOf[CommandLineProgramTesting]), subCommandClazz=None))
      output should include(s"No option found with name '${args.head.substring(2)}'")
    }
  }

  it should "print just the command usage when an unknown clp name is passed to command" in {
    Stream(
      Array[String]("--", "CommandLineProgramFive"),
      Array[String]("--", "CommandLineProgramFive", "--flag")
    ).foreach { args =>
      val (parser, commandOption, clpOption, output) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramTesting,CommandLineProgramOne](args)
      commandOption shouldBe 'empty
      clpOption shouldBe 'empty
      output should include(parser.standardCommandAndSubCommandUsagePreamble(commandClazz=Some(classOf[CommandLineProgramTesting]), subCommandClazz=None))
      output should include(parser.unknownSubCommandErrorMessage("CommandLineProgramFive"))
    }
  }

  it should "print just the command usage when command's custom command line validation fails" in {
    Stream(
      Array[String](nameOf(classOf[CommandLineProgramOne]))
    ).foreach { args =>
      val (parser, commandOption, clpOption, output) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramValidionError,CommandLineProgramOne](args)
      commandOption shouldBe 'empty
      clpOption shouldBe 'empty
      output should include(parser.standardCommandAndSubCommandUsagePreamble(commandClazz=Some(classOf[CommandLineProgramValidionError]), subCommandClazz=None))
      output should not include classOf[ValidationException].getSimpleName
      output should include("WTF")
    }
  }

  it should "print the command and clp usage when only the clp name is given" in {
    val args = Array[String](nameOf(classOf[CommandLineProgramThree]))
    val (parser, commandOption, clpOption, output) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramTesting,CommandLineProgramThree](args)
    commandOption shouldBe 'empty
    clpOption shouldBe 'empty
    TestParsecommandAndClp.checkEmptyClpUsage[CommandLineProgramTesting,CommandLineProgramThree](parser=parser, commandOption=commandOption, clpOption=clpOption, output=output)
  }

  it should "print the command and clp usage when only the clp name separator \"--\" and clp name are given" in {
    val args = Array[String]("--", nameOf(classOf[CommandLineProgramThree]))
    val (parser, commandOption, clpOption, output) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramTesting,CommandLineProgramThree](args)
    commandOption shouldBe 'empty
    clpOption shouldBe 'empty
    TestParsecommandAndClp.checkEmptyClpUsage[CommandLineProgramTesting,CommandLineProgramThree](parser=parser, commandOption=commandOption, clpOption=clpOption, output=output)
  }

  it should "print the command and clp usage when only the clp name and -h are given" in {
    val args = Array[String](nameOf(classOf[CommandLineProgramThree]), "-h")
    val (parser, commandOption, clpOption, output) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramTesting,CommandLineProgramThree](args)
    commandOption shouldBe 'empty
    clpOption shouldBe 'empty
    TestParsecommandAndClp.checkEmptyClpUsage[CommandLineProgramTesting,CommandLineProgramThree](parser=parser, commandOption=commandOption, clpOption=clpOption, output=output)
  }

  it should "print just the command version when -v or --version with a clp name" in {
    Stream("-v", "--version").foreach { arg =>
      val args = Array[String](nameOf(classOf[CommandLineProgramThree]), arg)
      val (parser, commandOption, clpOption, output) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramTesting,CommandLineProgramThree](args)
      commandOption shouldBe 'empty
      clpOption shouldBe 'empty
      val version = new CommandLineProgramParser(classOf[CommandLineProgramTesting]).version
      output should include(version)
    }
  }

  it should "print the command and clp usage when only the clp name separator \"--\" and clp name and -h are given" in {
    val args = Array[String]("--", nameOf(classOf[CommandLineProgramThree]), "-h")
    val (parser, commandOption, clpOption, output) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramTesting,CommandLineProgramThree](args)
    commandOption shouldBe 'empty
    clpOption shouldBe 'empty
    TestParsecommandAndClp.checkEmptyClpUsage[CommandLineProgramTesting,CommandLineProgramThree](parser=parser, commandOption=commandOption, clpOption=clpOption, output=output)
  }

  it should "print the command and clp usage when unknown clp arguments are given " in {
    Stream(
      Array[String]("--", nameOf(classOf[CommandLineProgramThree]), "--blarg", "4"),
      Array[String](nameOf(classOf[CommandLineProgramThree]), "--blarg", "4"),
      Array[String](nameOf(classOf[CommandLineProgramThree]), nameOf(classOf[CommandLineProgramTwo]))
    ).foreach { args =>
      val (parser, commandOption, clpOption, output: String) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramTesting,CommandLineProgramThree](args)
      commandOption shouldBe 'empty
      clpOption shouldBe 'empty
      TestParsecommandAndClp.checkEmptyClpUsage[CommandLineProgramTesting,CommandLineProgramThree](parser=parser, commandOption=commandOption, clpOption=clpOption, output=output)
    }
  }

  it should "print the command and clp usage when required clp arguments are not specified" in {
    Stream(
      Array[String]("--", nameOf(classOf[CommandLineProgramThree])),
      Array[String](nameOf(classOf[CommandLineProgramThree]))
    ).foreach { args =>
      val (parser, commandOption, clpOption, output) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramTesting,CommandLineProgramThree](args)
      commandOption shouldBe 'empty
      clpOption shouldBe 'empty
      output should include(classOf[UserException].getSimpleName)
      output should include(CommandLineProgramParserStrings.requiredArgumentErrorMessage("argument"))
    }
  }

  it should "print the command and clp usage when clp arguments are specified but are mutually exclusive" in {
    Stream(
      Array[String]("--", nameOf(classOf[CommandLineProgramWithMutex]), "--argument", "value", "--another", "value"),
      Array[String](nameOf(classOf[CommandLineProgramWithMutex]), "--argument", "value", "--another", "value")
    ).foreach { args =>
      val (parser, commandOption, clpOption, output) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramTesting,CommandLineProgramWithMutex](args)
      commandOption shouldBe 'empty
      clpOption shouldBe 'empty
      output should include(classOf[UserException].getSimpleName)
      output should include(ClpArgumentDefinitionPrinting.mutexErrorHeader)
    }
  }

  it should "return a valid clp with and without using the clp name separator \"--\" and valid arguments" in {
    Stream(
      //Array[String]("--", getName(classOf[PipelineThree]), "--argument", "value"),
      Array[String](nameOf(classOf[CommandLineProgramThree]), "--argument", "value")
    ).foreach { args =>
      val (parser, commandOption, clpOption, output) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramTesting,CommandLineProgramThree](args)
      commandOption shouldBe 'defined
      commandOption.get.getClass shouldBe classOf[CommandLineProgramTesting]
      clpOption shouldBe 'defined
      clpOption.get.getClass shouldBe classOf[CommandLineProgramThree]
      clpOption.get.argument shouldBe "value"
      output shouldBe 'empty
    }
  }

  it should "return a valid clp without using the clp name separator \"--\" and no arguments with a clp that requires no arguments" in {
    Stream(
      Array[String]("--", nameOf(classOf[CommandLineProgramFour])),
      Array[String](nameOf(classOf[CommandLineProgramFour]))
    ).foreach { args =>
      val (parser, commandOption, clpOption, output) = TestParsecommandAndClp.parseCommandAndClp[CommandLineProgramTesting,CommandLineProgramFour](args)
      commandOption shouldBe 'defined
      commandOption.get.getClass shouldBe classOf[CommandLineProgramTesting]
      clpOption shouldBe 'defined
      clpOption.get.getClass shouldBe classOf[CommandLineProgramFour]
      clpOption.get.argument shouldBe "default"
      output shouldBe 'empty
      output shouldBe 'empty
    }
  }

  "CommandLineParser.clpListUsage" should "throw a BadAnnotationException when a class without the @arg is given" in {
    val parser = new CommandLineParser[Seq[String]]("command-line-name")
    val classes =  Set[Class[_ <: Seq[String]]](classOf[Seq[String]], classOf[List[String]])
    an[BadAnnotationException] should be thrownBy parser.subCommandListUsage(classes, "clp", withPreamble=true)
  }

  private object TestSplitArgs {
    val parser = new CommandLineParser[CommandLineProgram]("CommandLineName")
    val packageList = List("dagr.sopt.cmdline.testing.clps")

    def testSplitArgs(args: Array[String], commandArgsSize: Int, clpArgsSize: Int): Unit = {
      val (commandArgs, clpArgs) = parser.splitArgs(
        args,
        packageList,
        List(),
        includeHidden=true
      )
      commandArgs should have size commandArgsSize
      clpArgs should have size clpArgsSize
    }

    private def nameOf(clazz: Class[_]): String = clazz.getSimpleName
  }

  "CommandLineParser.splitArgs" should "find \"--\" when no command name is given" in {
    import TestSplitArgs._
    testSplitArgs(Array[String]("--"), 0, 0)
    testSplitArgs(Array[String]("blah", "--"), 1, 0)
    testSplitArgs(Array[String]("blah", "--", "blah"), 1, 1)
  }

  it should "find \"--\" first when a command name is also given" in {
    import TestSplitArgs._
    testSplitArgs(Array[String]("--", nameOf(classOf[CommandLineProgramOne])), 0, 1)
    testSplitArgs(Array[String](nameOf(classOf[CommandLineProgramOne]), "--"), 1, 0)
    testSplitArgs(Array[String]("blah", "--", nameOf(classOf[CommandLineProgramOne])), 1, 1)
  }

  it should "find the command name when no \"--\" is given" in {
    import TestSplitArgs._
    testSplitArgs(Array[String](nameOf(classOf[CommandLineProgramOne])), 0, 1)
    testSplitArgs(Array[String]("blah", nameOf(classOf[CommandLineProgramOne])), 1, 1)
    testSplitArgs(Array[String]("blah", nameOf(classOf[CommandLineProgramOne]), "blah"), 1, 2)
  }
}
