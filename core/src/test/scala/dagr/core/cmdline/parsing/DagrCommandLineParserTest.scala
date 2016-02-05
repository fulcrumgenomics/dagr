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

import dagr.core.config.Configuration
import dagr.core.util.{CaptureSystemStreams, Logger, LogLevel, UnitSpec}
import dagr.core.cmdline.{PipelineClass, UserException, DagrCoreMain}
import dagr.core.cmdline.parsing.testing.pipelines._
import dagr.core.tasksystem.Pipeline

class DagrCommandLineParserTest extends UnitSpec with CaptureSystemStreams {
  import DagrCommandLineParserStrings._
  import CommandLineParserStrings._

  private object TestSplitArgsData {
    val parser = new DagrCommandLineParser("CommandLineName")
    val packageList = List("dagr.core.cmdline.parsing.testing.pipelines")
  }

  private def testSplitArgs(args: Array[String], dagrArgsSize: Int, taskArgsSize: Int) : Unit = {
    val dagrErrorMessageBuilder: StringBuilder = new StringBuilder
    val (dagrArgs, taskArgs) = TestSplitArgsData.parser.splitArgs(
      args,
      TestSplitArgsData.packageList,
      List(),
      dagrErrorMessageBuilder,
      includeHidden=true
    )
    dagrArgs should have size dagrArgsSize
    taskArgs should have size taskArgsSize
  }

  private def nameOf(clazz: PipelineClass): String = clazz.getSimpleName

  "DagrCommandLineParser.splitArgs" should "find \"--\" when no Pipeline name is given" in {
    testSplitArgs(Array[String]("--"), 0, 0)
    testSplitArgs(Array[String]("blah", "--"), 1, 0)
    testSplitArgs(Array[String]("blah", "--", "blah"), 1, 1)
  }

  it should "find \"--\" first when a Pipeline name is also given" in {
    testSplitArgs(Array[String]("--", nameOf(classOf[PipelineOne])), 0, 1)
    testSplitArgs(Array[String](nameOf(classOf[PipelineOne]), "--"), 1, 0)
    testSplitArgs(Array[String]("blah", "--", nameOf(classOf[PipelineOne])), 1, 1)
  }

  it should "find the Pipeline name when no \"--\" is given" in {
    testSplitArgs(Array[String](nameOf(classOf[PipelineOne])), 0, 1)
    testSplitArgs(Array[String]("blah", nameOf(classOf[PipelineOne])), 1, 1)
    testSplitArgs(Array[String]("blah", nameOf(classOf[PipelineOne]), "blah"), 1, 2)
  }

  private object TestParseDagrArgsData {
    val packageList: List[String] = List[String]("dagr.core.cmdline.parsing.testing.pipelines")

    def parseDagrArgs(args: Array[String]): (Option[_ <: Pipeline], String) = {
      val parser = new DagrCommandLineParser({
        Configuration.commandLineName
      }, includeHidden = true)
      Logger.synchronized { // since we modify the log level
        val logLevel = Logger.level
        Logger.level = LogLevel.Fatal // turn off all logging
        var taskOption: Option[_ <: Pipeline] = None
        var dagrOption: Option[DagrCoreMain] = None
        val output: String = captureStderr(() => {
          parser.parse(args, packageList) match {
            case Some((dagr, pipeline)) =>
              dagrOption = Some(dagr)
              taskOption = Some(pipeline)
            case None => Unit
          }
        })
        Logger.level = logLevel // turn logging back on
        (taskOption, output)
      }
    }
  }

  def checkEmptyDagrUsage(taskOption: Option[_ <: Pipeline], output: String): Unit = {
    taskOption shouldBe 'empty
    output should include(s"$USAGE_PREFIX ${Configuration.commandLineName}")
    output should include(s"${Configuration.commandLineName} $OPTIONAL_ARGUMENTS")
    output should include(AVAILABLE_PIPELINES)
    output should not include DagrCoreMain.buildErrorMessage()
  }

  def checkEmptyTaskUsage(taskOption: Option[_ <: Pipeline], output: String, taskClazz: PipelineClass): Unit = {
    taskOption shouldBe 'empty
    output should include(s"$USAGE_PREFIX ${Configuration.commandLineName}")
    output should include(s"${Configuration.commandLineName} $OPTIONAL_ARGUMENTS")
    output should include(s"${nameOf(taskClazz)} $REQUIRED_ARGUMENTS")
    output should include(s"${nameOf(taskClazz)} $OPTIONAL_ARGUMENTS")
  }

  "DagrCommandLineParser.parseDagrArgs" should "print just the dagr usage when no arguments or the flag are/is given" in {
    Stream(Array[String](), Array[String]("-h"), Array[String]("--help")).foreach { args =>
      val (taskOption, output) = TestParseDagrArgsData.parseDagrArgs(args)
      checkEmptyDagrUsage(taskOption, output)
      if (args.isEmpty) {
        output should include(getUnknownPipeline(Configuration.commandLineName))
      }
      output should not include DagrCoreMain.buildErrorMessage()
    }
  }

  it should "print just the dagr usage when only dagr and pipeline separator \"--\" is given" in {
    val (taskOption, output) = TestParseDagrArgsData.parseDagrArgs(Array[String]("--"))
    checkEmptyDagrUsage(taskOption, output)
    output should include(getUnknownPipeline(Configuration.commandLineName))
    output should not include DagrCoreMain.buildErrorMessage(None, None)
  }

  it should "print just the dagr usage when unknown arguments are passed to dagr" in {
    Stream(
      Array[String]("--helloWorld", "CommandLineTaskTesting"),
      Array[String]("---", "CommandLineTaskTesting")
    ).foreach { args =>
      val (taskOption, output) = TestParseDagrArgsData.parseDagrArgs(args)
      taskOption shouldBe 'empty
      output should include(s"$USAGE_PREFIX ${Configuration.commandLineName}")
      output should include(s"${Configuration.commandLineName} $OPTIONAL_ARGUMENTS")
      output should include(s"No option found for name '${args.head.substring(2)}'")
      output should not include DagrCoreMain.buildErrorMessage()
    }
  }

  it should "print just the dagr usage when an unknown pipeline name is passed to dagr" in {
    Stream(
      Array[String]("PipelineFive"),
      Array[String]("--", "PipelineFive"),
      Array[String]("--", "PipelineFive", "--flag")
    ).foreach { args =>
      val (taskOption, output) = TestParseDagrArgsData.parseDagrArgs(args)
      taskOption shouldBe 'empty
      output should include(s"$USAGE_PREFIX ${Configuration.commandLineName}")
      output should include(s"${Configuration.commandLineName} $OPTIONAL_ARGUMENTS")
      if (args.length == 1) {
        output should include(s"${getUnknownPipeline({Configuration.commandLineName})}")
      }
      else {
        output should include(getUnknownCommand("PipelineFive"))
      }
      output should not include DagrCoreMain.buildErrorMessage()
    }
  }

  it should "print just the dagr usage when dagr's custom command line validation fails" in {
    Stream(
      Array[String]("--script-dir", "/path/to/nowhere", nameOf(classOf[PipelineFour]))
    ).foreach { args =>
      val (taskOption, output) = TestParseDagrArgsData.parseDagrArgs(args)
      taskOption shouldBe 'empty
      output should include(s"$USAGE_PREFIX ${Configuration.commandLineName}")
      output should include(s"${Configuration.commandLineName} $OPTIONAL_ARGUMENTS")
      output should include("/path/to/nowhere")
      output should include(DagrCoreMain.buildErrorMessage())
    }
  }

  it should "print just the dagr usage when the path to the dagr configuration file does not exist" in {
    Stream(
      Array[String]("--config", "/path/to/nowhere", nameOf(classOf[PipelineFour]))
    ).foreach { args =>
      val (taskOption, output) = TestParseDagrArgsData.parseDagrArgs(args)
      taskOption shouldBe 'empty
      output should include(s"$USAGE_PREFIX ${Configuration.commandLineName}")
      output should include(s"${Configuration.commandLineName} $OPTIONAL_ARGUMENTS")
      output should include(s"${classOf[java.io.FileNotFoundException].getCanonicalName}: /path/to/nowhere")
    }
  }

  it should "print the dagr and task usage when only the pipeline name is given" in {
    val (taskOption, output) = TestParseDagrArgsData.parseDagrArgs(Array[String](nameOf(classOf[PipelineThree])))
    checkEmptyTaskUsage(taskOption, output, classOf[PipelineThree])
  }

  it should "print the dagr and task usage when only the pipeline name separator \"--\" and pipeline name are given" in {
    val (taskOption, output) = TestParseDagrArgsData.parseDagrArgs(Array[String]("--", nameOf(classOf[PipelineThree])))
    checkEmptyTaskUsage(taskOption, output, classOf[PipelineThree])
  }

  it should "print the dagr and task usage when only the pipeline name and -h are given" in {
    val (taskOption, output) = TestParseDagrArgsData.parseDagrArgs(Array[String](nameOf(classOf[PipelineThree]), "-h"))
    checkEmptyTaskUsage(taskOption, output, classOf[PipelineThree])
  }

  it should "print the dagr and task usage when only the pipeline name separator \"--\" and pipeline name and -h are given" in {
    val (taskOption, output) = TestParseDagrArgsData.parseDagrArgs(Array[String]("--", nameOf(classOf[PipelineThree]), "-h"))
    checkEmptyTaskUsage(taskOption, output, classOf[PipelineThree])
  }

  it should "print the dagr and task usage when unknown pipeline arguments are given " in {
    Stream(
      Array[String]("--", nameOf(classOf[PipelineThree]), "--blarg", "4"),
      Array[String](nameOf(classOf[PipelineThree]), "--blarg", "4"),
      Array[String](nameOf(classOf[PipelineThree]), nameOf(classOf[PipelineTwo]))
    ).foreach { args =>
      val (taskOption, output) = TestParseDagrArgsData.parseDagrArgs(args)
      checkEmptyTaskUsage(taskOption, output, classOf[PipelineThree])
    }
  }

  it should "print the dagr and task usage when required task arguments are not specified" in {
    Stream(
      Array[String]("--", nameOf(classOf[PipelineThree])),
      Array[String](nameOf(classOf[PipelineThree]))
    ).foreach { args =>
      val (taskOption, output) = TestParseDagrArgsData.parseDagrArgs(args)
      taskOption shouldBe 'empty
      output should include(classOf[UserException].getSimpleName)
      output should include(getRequiredArgument("argument"))
    }
  }

  it should "return a valid task with and without using the pipeline name separator \"--\" and valid arguments" in {
    Stream(
      //Array[String]("--", getName(classOf[PipelineThree]), "--argument", "value"),
      Array[String](nameOf(classOf[PipelineThree]), "--argument", "value")
    ).foreach { args =>
      val (taskOption, output) = TestParseDagrArgsData.parseDagrArgs(args)
      taskOption shouldBe 'defined
      taskOption.get.getClass shouldBe classOf[PipelineThree]
      taskOption.get.asInstanceOf[PipelineThree].argument shouldBe "value"
      output shouldBe 'empty
    }
  }

  it should "return a valid task with and without using the pipeline name separator \"--\" and no arguments with a pipeline that requires no arguments" in {
    Stream(
      Array[String]("--", nameOf(classOf[PipelineFour])),
      Array[String](nameOf(classOf[PipelineFour]))
    ).foreach { args =>
      val (taskOption, output) = TestParseDagrArgsData.parseDagrArgs(args)
      taskOption shouldBe 'defined
      taskOption.get.getClass shouldBe classOf[PipelineFour]
      taskOption.get.asInstanceOf[PipelineFour].argument shouldBe "default"
      taskOption.get.asInstanceOf[PipelineFour].flag shouldBe false
      output shouldBe 'empty
    }
  }

  // TODO:
  // Test Dagr scripts
  // Verify all the tests relating to pipeline names but using a Dagr script instead of pre-built pipeline.
  // Verify mutexes between --quiet, --verbosity, and --debug.
}
