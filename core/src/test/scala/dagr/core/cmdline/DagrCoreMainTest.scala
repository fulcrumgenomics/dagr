/*
 * The MIT License
 *
 * Copyright (c) 2016 Fulcrum Genomics LLC
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

package dagr.core.cmdline

import dagr.commons.util.{CaptureSystemStreams, LogLevel, Logger, UnitSpec}
import dagr.core.cmdline.pipelines.PipelineFour
import dagr.core.tasksystem.{NoOpInJvmTask, Pipeline}
import dagr.commons.io.Io
import dagr.sopt.util.TermCode
import org.scalatest.BeforeAndAfterAll

class NoOpPipeline extends Pipeline {
  override def build(): Unit = {
    root ==> new NoOpInJvmTask("NoOpInJvmTask")
  }
}

class DagrCoreMainTest extends UnitSpec with CaptureSystemStreams with BeforeAndAfterAll {

  private val prevPrintColor = TermCode.printColor
  override protected def beforeAll(): Unit = TermCode.printColor = false
  override protected def afterAll(): Unit = TermCode.printColor = prevPrintColor

  // required so that colors are not in our usage messages
  "DagrCoreMainTest" should "have color status of Dagr be false" in {
    // this should be set in the build.sbt
    TermCode.printColor shouldBe false
  }

  "DagrCoreMain.execute" should "run a pipeline end-to-end" in {
    val clp = new DagrCoreMain(logLevel=LogLevel.Fatal, report=Some(Io.DevNull))
    val pipeline = new NoOpPipeline()

    clp.configure(pipeline)
    clp.execute(pipeline) shouldBe 0

    // NB: need to set the log level back
    Logger.level = LogLevel.Info
  }

  it should "run a pipeline end-to-end in interactive mode" in {
    val clp = new DagrCoreMain(logLevel=LogLevel.Fatal, report=Some(Io.DevNull), interactive=true)
    val pipeline = new NoOpPipeline()

    clp.configure(pipeline)
    val stdout = captureStdout(() => {
      clp.execute(pipeline) shouldBe 0
    })
    stdout should include("1 Done")

    // NB: need to set the log level back
    Logger.level = LogLevel.Info
  }

  private def nameOf(clazz: Class[_]): String = clazz.getSimpleName

  private def testParse(args: Array[String]): (Option[DagrCoreMain], Option[Pipeline], String, String) = {
    var mainOption: Option[DagrCoreMain] = None
    var pipelineOption: Option[Pipeline] = None
    var stdout: String = ""
    val stderr: String = captureStderr(() => {
      stdout = captureStdout(() => {
        DagrCoreMain.parse(args=args, packageList=List("dagr.core.cmdline.pipelines"), includeHidden=true) match {
          case Some((m, p)) =>
            mainOption = Some(m)
            pipelineOption = Some(p)
          case None => Unit
        }
      })
    })
    (mainOption, pipelineOption, stderr, stdout)
  }

  "DagrCoreMain.parse" should "print just the main usage when the path to the main configuration file does not exist" in {
    Stream(
      Array[String]("--config", "/path/to/nowhere", nameOf(classOf[PipelineFour]))
    ).foreach { args =>
      val (_, _, stderr, _) = testParse(args)
      stderr should include ("/path/to/nowhere")
    }
  }

  // FIXME
  /*
  it should "load in a dagr script successfully" in {
    val (tmpDir: Path, tmpFile: Path) = DagrScriptManagerTest.writeScript(DagrScriptManagerTest.helloWorldScript)
    val args: Array[String] = Array[String]("--scripts", tmpFile.toAbsolutePath.normalize().toString)
    val (mainOption, pipelineOption, stderr, stdout) = testParse(args)
    println("stdout: " + stdout)
    println("stderr:" + stderr)
    stdout should include("Compilation complete")
  }

  it should "fail to compile a buggy dagr script" in {
    val (tmpDir: Path, tmpFile: Path) = DagrScriptManagerTest.writeScript("ABCDEFG\n" + DagrScriptManagerTest.helloWorldScript)
    val args: Array[String] = Array[String]("--scripts", tmpFile.toAbsolutePath.normalize().toString)
    val (mainOption, pipelineOption, stderr, stdout) = testParse(args)
    println("stdout: " + stdout)
    println("stderr:" + stderr)
    stderr should include("failed")
  }
  */

  // TODO: test using loading up a config file on the command line
}
