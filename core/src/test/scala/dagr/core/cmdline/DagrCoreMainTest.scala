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

import java.nio.file.{Files, Path}

import com.fulcrumgenomics.commons.io.Io
import com.fulcrumgenomics.commons.util.CaptureSystemStreams
import com.fulcrumgenomics.sopt.util.TermCode
import dagr.core.FutureUnitSpec
import dagr.core.cmdline.DagrScriptManager.DagrScriptManagerException
import dagr.core.cmdline.pipelines.PipelineFour
import dagr.core.tasksystem.{NoOpInJvmTask, Pipeline}
import org.scalatest.BeforeAndAfterAll

private class NoOpPipeline extends Pipeline {
  override def build(): Unit = {
    root ==> new NoOpInJvmTask("NoOpInJvmTask")
  }
}

class DagrCoreMainTest extends FutureUnitSpec with CaptureSystemStreams with BeforeAndAfterAll {

  private val prevPrintColor = TermCode.printColor

  private def nameOf(clazz: Class[_]): String = clazz.getSimpleName

  private def packageName: String = "dagr.core.cmdline.pipelines"

  private def reportPath: Path = {
    val path = Files.createTempFile("DagrCoreMain.", ".report.txt")
    path.toFile.deleteOnExit()
    path
  }

  private def testParse(args: Array[String]): (Option[Int], StdErrString, StdOutString, LoggerString) = {
    var exitCode: Option[Int] = None
    val (stderr, stdout, log) = captureItAll(() => {
      val argsWithReport = Array[String]("--report", reportPath.toAbsolutePath.toString) ++ args
      exitCode = Some(new DagrCoreMain[DagrCoreArgs]().makeItSo(args=argsWithReport, packageList=List(packageName), includeHidden=true))
    })
    (exitCode, stderr, stdout, log)
  }

  override protected def beforeAll(): Unit = TermCode.printColor = false

  override protected def afterAll(): Unit = TermCode.printColor = prevPrintColor

  // required so that colors are not in our usage messages
  "DagrCoreMainTest" should "have color status of Dagr be false" in {
    // this should be set in the build.sbt
    TermCode.printColor shouldBe false
  }

  "DagrCoreArgs.execute" should "run a pipeline end-to-end" in {
    captureLogger(() => {
      val clp = new DagrCoreArgs(report = Some(Io.DevNull))
      val pipeline = new NoOpPipeline()

      clp.configure(pipeline)
      clp.execute(pipeline) shouldBe 0
    })
  }

  it should "run a pipeline end-to-end in interactive mode" in {
    Seq(true, false).foreach { experimentalExecution =>
      Seq(true, false).foreach { withLogAndScriptDir =>

        val logAndScriptDir = if (!withLogAndScriptDir) None else {
          val dir = Files.createTempDirectory("DagrCoreMainTest")
          dir.toFile.deleteOnExit()
          Some(dir)
        }

        captureLogger(() => {
          val clp = new DagrCoreArgs(
            report                = Some(Io.DevNull),
            interactive           = true,
            scriptDir             = logAndScriptDir,
            logDir                = logAndScriptDir,
            experimentalExecution = experimentalExecution
          )
          val pipeline  = new NoOpPipeline()

          clp.configure(pipeline)
          val stdout = captureStdout(() => {
            clp.execute(pipeline) shouldBe 0
          })
          stdout should include("1 Done")
        })
      }
    }
  }

  "DagrCoreMain.parse" should "print just the main usage when the path to the main configuration file does not exist" in {
    val (_, stderr, _, _) = testParse(Array[String]("--config", "/path/to/nowhere", nameOf(classOf[PipelineFour])))
    stderr should include ("/path/to/nowhere")
  }

  it should "accept a configuration file" in {
    val path = Files.createTempFile("DagrCoreMain.", ".config.txt")
    path.toFile.deleteOnExit()
    Io.writeLines(path,
      Seq("""
            |dagr = {
            |  command-line-name = "test-name"
            |}
      """.stripMargin
      )
    )

    val (_, _, _, log) = testParse(Array[String]("--config", path.toAbsolutePath.toString, nameOf(classOf[PipelineFour])))
    log should include("test-name")
  }

  it should "print the execution failure upon failure" in {
    val (_, _, _, log) = testParse(Array[String](nameOf(classOf[PipelineFour])))
    log should include("Elapsed time:")
    log should include("dagr failed")
  }

  it should "load in a dagr script successfully" in {
    val (_, tmpFile: Path) = DagrScriptManagerTest.writeScript(DagrScriptManagerTest.helloWorldScript(packageName))
    val args: Array[String] = Array[String](s"--scripts", tmpFile.toAbsolutePath.normalize().toString, "HelloWorldPipeline")
    val (_, _, _, log) = testParse(args)
    log should include("Compilation complete")
  }

  it should "fail to compile a buggy dagr script" in {
    val (_, tmpFile: Path) = DagrScriptManagerTest.writeScript("ABCDEFG\n" + DagrScriptManagerTest.helloWorldScript(packageName))
    val args: Array[String] = Array[String]("--scripts", tmpFile.toAbsolutePath.normalize().toString)

    val exception = intercept[DagrScriptManagerException] {
      testParse(args)
    }
    exception.getMessage should include(s"Compile of $tmpFile failed with")
  }
}
