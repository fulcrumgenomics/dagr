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

package dagr.tasks.parallel
import dagr.commons.io.Io

import java.nio.file.{Path, Files}

import dagr.commons.util.{LogLevel, Logger, UnitSpec}
import dagr.core.execsystem.{SystemResources, TaskManager}
import dagr.core.tasksystem.SimpleInJvmTask
import org.scalatest.BeforeAndAfterAll

class PathBasesScatterGatherPipelineTest extends UnitSpec with BeforeAndAfterAll {

  override def beforeAll(): Unit = Logger.level = LogLevel.Fatal
  override def afterAll(): Unit = Logger.level = LogLevel.Info

  def getDefaultTaskManager: TaskManager = new TaskManager(taskManagerResources = SystemResources.infinite, scriptsDirectory = None, sleepMilliseconds=1)

  private class SplitByLineTask(input: Path, tmpDirectory: Path) extends SimpleInJvmTask with SplitInputTask[Path, Path] {
    def run(): Unit = {
      val lines = Io.readLines(input)
      _subDomains = Some(lines.map { line =>
        val splitInputPath = Files.createTempFile(tmpDirectory, "split.input", ".txt")
        Io.writeLines(splitInputPath, Seq(line))
        splitInputPath
      }.toSeq)
    }
  }

  private class CountWordsTask(input: Path, tmpDirectory: Path) extends SimpleInJvmTask with ScatterTask[Path, Path] {
    def run(): Unit = {
      val output = Files.createTempFile(tmpDirectory, "split.output", ".txt")
      val lines = Io.readLines(input).map {
        line => line.split(" ").length + " " + line
      }
      Io.writeLines(output, lines.toSeq)
      _gatheredOutput = Some(output)
    }
  }

  private class GatherByLineTask(inputs: Seq[Path], output: Path) extends SimpleInJvmTask with GatherTask[Path] {
    def run(): Unit = {
      val lines = inputs.flatten { in => Io.readLines(in) }
      Io.writeLines(output, lines.toSeq)
    }
    def gatheredOutput: Path = output
  }

  "PathBasedScatterGatherPipeline" should "run a simple path-based scatter-gather" in {
    val taskManager = getDefaultTaskManager

    // setup the input and output
    val tmpDirectory = Files.createTempDirectory("PathBasedScatterGatherPipelineTest")
    val input = Files.createTempFile(tmpDirectory, "input", ".txt")
    val output = Files.createTempFile(tmpDirectory, "output", ".txt")
    val str = "a b c d e f g h i j k l m n o p q r s t u v w x y z ä å ö"
    Io.writeLines(input, Seq(str, str, str, str, str))

    val pipeline = new PathBasedScatterGatherPipeline(
      domain                      = input,
      output                      = output,
      splitInputPathTaskGenerator = (tmpDirectory: Option[Path]) => (input: Path) => new SplitByLineTask(input=input, tmpDirectory=tmpDirectory.get),
      scatterTaskGenerator        = (tmpDirectory: Option[Path]) => (input: Path) => new CountWordsTask(input=input, tmpDirectory=tmpDirectory.get),
      gatherTaskGenerator         = (inputs: Seq[Path], output: Path)             => new GatherByLineTask(inputs=inputs, output=output),
      tmpDirectory                = Some(tmpDirectory)
    )

    taskManager.addTask(pipeline)
    taskManager.runToCompletion(true)

    val outputLines = Io.readLines(output)
    outputLines should have size 5
    outputLines.foreach { line => line shouldBe ("5 " + str) }
  }
}
