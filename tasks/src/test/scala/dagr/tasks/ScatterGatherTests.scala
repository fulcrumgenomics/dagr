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

package dagr.tasks

import java.nio.file.{Files, Path}

import com.fulcrumgenomics.commons.io.Io
import com.fulcrumgenomics.commons.util.{LazyLogging, LogLevel, Logger}
import dagr.core.execsystem.{SystemResources, TaskManager}
import dagr.core.tasksystem.{Pipeline, SimpleInJvmTask}
import dagr.tasks.ScatterGather.{Partitioner, Scatter}
import org.scalatest.BeforeAndAfterAll

/**
  * Tests for the scatter gather task framework.
  *
  * Our test involves a "complicated" little scatter/gather that does the following:
  *   - Scatter a text file by lines
  *   - Count the words on each line
  *     - [Gather] Sum the counts
  *   - Square the counts
  *     - [Gather] Sum the squares
  **/
class ScatterGatherTests extends UnitSpec with LazyLogging with BeforeAndAfterAll {
  override def beforeAll(): Unit = Logger.level = LogLevel.Fatal
  override def afterAll(): Unit = Logger.level = LogLevel.Info
  def buildTaskManager: TaskManager = new TaskManager(taskManagerResources = SystemResources.infinite, scriptsDirectory = None, sleepMilliseconds=1, failFast=true)

  def tmp(): Path = {
    val path = Files.createTempFile("testScatterGather.", ".txt")
    path.toFile.deleteOnExit()
    path
  }

  /** Splits a file into one file per line. */
  private case class SplitByLine(input: Path) extends SimpleInJvmTask with Partitioner[Path] {
    var partitions: Option[Seq[Path]] = None

    def run(): Unit = {
      val lines = Io.readLines(input).toSeq
      val paths = lines.map(_ => tmp())
      lines.zip(paths) foreach { case(line, path) => Io.writeLines(path, Seq(line))}
      this.partitions = Some(paths)
    }
  }

  private case class CountWords(input: Path, output: Path) extends SimpleInJvmTask {
    def run(): Unit = Io.writeLines(output, Io.readLines(input).toSeq.map(_.split(" ").length.toString))
  }

  private case class SquareNumbers(input: Path, output:Path) extends SimpleInJvmTask {
    def run(): Unit = Io.writeLines(output, Io.readLines(input).toSeq.map(x => x.toInt * x.toInt).map(_.toString))
  }

  private case class SumNumbers(inputs: Seq[Path], output: Path) extends SimpleInJvmTask {
    def run(): Unit = Io.writeLines(output, Seq(inputs.flatMap(Io.readLines).map(_.toInt).sum.toString))
  }

  val lines = Seq("one", "one two", "one two three", "one two three four", "one two three four five")
  val lengths = Seq(1,2,3,4,5)

  "ScatterGather" should "run a simple scatter-gather pipeline on files" in {
    // setup the input and output
    val input = tmp()
    val sumOfCounts  = tmp()
    val sumOfSquares = tmp()
    Io.writeLines(input, lines)

    val pipeline = new Pipeline() {
      override def build(): Unit = {
        val scatter = Scatter(SplitByLine(input=input))
        val counts = scatter.map(p => CountWords(input=p, output=tmp()))
        counts.gather(cs => SumNumbers(inputs=cs.map(_.output), output=sumOfCounts))

        val squares = counts.map(c => SquareNumbers(input=c.output, output=tmp()))
        squares.gather(ss => SumNumbers(ss.map(_.output), output=sumOfSquares))

        root ==> scatter
      }
    }

    val taskManager = buildTaskManager
    taskManager.addTask(pipeline)
    taskManager.runToCompletion()

    val sum1 = Io.readLines(sumOfCounts).next().toInt
    val sum2 = Io.readLines(sumOfSquares).next().toInt

    sum1 shouldBe lengths.sum
    sum2 shouldBe lengths.map(x => x*x).sum
  }
}
