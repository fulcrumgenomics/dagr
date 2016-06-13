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

package dagr.tasks.parallel

import java.io.PrintWriter
import java.nio.file.{Files, Path}

import dagr.commons.util.{LogLevel, Logger, UnitSpec}
import dagr.core.execsystem.{SystemResources, TaskManager}
import dagr.core.tasksystem.SimpleInJvmTask
import dagr.commons.io.Io
import org.scalatest.BeforeAndAfterAll

class ScatterGatherPipelineTest extends UnitSpec with BeforeAndAfterAll {

  override def beforeAll(): Unit = Logger.level = LogLevel.Fatal
  override def afterAll(): Unit = Logger.level = LogLevel.Info

  def getDefaultTaskManager: TaskManager = new TaskManager(taskManagerResources = SystemResources.infinite, scriptsDirectory = None, sleepMilliseconds=1)

  ////////////////////////////////////////////////////////////////////////////////
  // A simple example where we take a string, split it into a sequence of
  // characters, then feed each character to a separate scatter task, with each
  // scatter task returning one if the character is not a space (" "), zero
  // otherwise, and then gathering the values and summing them.
  ////////////////////////////////////////////////////////////////////////////////

  /** The task that generates the inputs for each scatter */
  class InputTask(val input: String) extends SimpleInJvmTask with SplitInputTask[String, Char] {
    withName("Inputs Task")
    override def run(): Unit = _subDomains = Some(input.toSeq)
  }

  /** The scatter task */
  class CountCharLength(val input: Char, ignoreCharacters: String = " ") extends SimpleInJvmTask with ScatterTask[Char, Int] {
    withName(s"Scatter Task: '$input'")
    override def run(): Unit = _gatheredOutput = Some(if (ignoreCharacters.contains(input)) 0 else 1)
  }

  /** The gather task */
  class SumTask(val inputs: Iterable[Int]) extends SimpleInJvmTask with GatherTask[Int] {
    withName("Gather Task")
    private var _output: Option[Int] = None
    override def gatheredOutput: Int = _output.get
    override def run(): Unit = _output = Some(inputs.sum[Int])
  }

  /** The base scatter gather */
  class ScatterGather(str: String)
    extends ScatterGatherPipeline[String, Char, Int] {
    def domain: String = str
    def splitDomainTask(input: String): SplitInputTask[String, Char] = new InputTask(input)
    def scatterTask(intermediate: Char): ScatterTask[Char, Int] = new CountCharLength(intermediate)
    def gatherTask(outputs: Iterable[Int]): GatherTask[Int] = new SumTask(inputs = outputs)
  }

  /** A scatter gather that performs hierarchical merging. */
  class MergingScatterGather(str: String, override val mergeSize: Int)
    extends ScatterGather(str = str)
    with MergingScatterGatherPipeline[String, Char, Int] {
  }

  "ScatterGatherTask" should "run a simple scatter gather" in {
    val taskManager = getDefaultTaskManager
    val str = "a b c d e f g h i j k l m n o p q r s t u v w x y z ä å ö"

    val simpleScatterGather = new ScatterGather(str = str)
    taskManager.addTask(simpleScatterGather)
    taskManager.runToCompletion(true)
    simpleScatterGather.gatheredOutput shouldBe 29
  }

  it should "run a simple scatter gather with the apply method" in {
    val taskManager = getDefaultTaskManager
    val str = "a b c d e f g h i j k l m n o p q r s t u v w x y z ä å ö"

    val simpleScatterGather = ScatterGatherPipeline[String, Char, Int](
      inDomain = str,
      toSubDomains = input => input.toSeq,
      toOutput = c => if (" ".contains(c)) 0 else 1,
      toFinalOutput = inputs => inputs.sum[Int]
    )
    taskManager.addTask(simpleScatterGather)
    taskManager.runToCompletion(true) 
    simpleScatterGather.gatheredOutput shouldBe 29
  }

  it should "run a merging scatter gather" in {
    val str = "a b c d e f g h i j k l m n o p q r s t u v w x y z ä å ö"
    // have mergeSize be large so that we can test if merging scatter gather reduces to a single gather
    for (mergeSize <- 2 until 29 by 9) {
      val taskManager = getDefaultTaskManager

      val mergingScatterGather = new MergingScatterGather(str = str, mergeSize = mergeSize)
      taskManager.addTask(mergingScatterGather)
      taskManager.runToCompletion(true)

      mergingScatterGather.gatheredOutput shouldBe 29
    }
  }

  ////////////////////////////////////////////////////////////////////////////////
  // A similar example to the non-space counting scatter gather, but instead, we
  // operate on files (eek!).  We also demonstrate how we can use the apply
  // methods.
  ////////////////////////////////////////////////////////////////////////////////

  def writeTmpFile(s: String): Path = {
    val tmpFile = Files.createTempFile("tmp", s)
    tmpFile.toFile.deleteOnExit()
    val pw = new PrintWriter(Io.toWriter(tmpFile))
    pw.write(s)
    pw.close()
    tmpFile
  }

  /** The method that generates the inputs for each scatter */
  def fileBasedToIntermediates(input: Path): Iterable[Path] = {
    scala.io.Source.fromFile(input.toFile)
        .mkString
        .replace("\n", "")
        .toSeq
        .map { c: Char => writeTmpFile(s"$c") }
  }

  /** The scatter method */
  def fileBasedToOutput(input: Path): Path = {
      val ignoreCharacters: String = " "
      val str = scala.io.Source.fromFile(input.toFile)
      val value = str.toSeq.map { c: Char => if (ignoreCharacters.contains(c)) 0 else 1 }.head
      writeTmpFile(value.toString)
  }

  /** The gather method */
  def fileBasedToFinalOutput(inputs: Iterable[Path]): Path = {
     val sum = inputs.map { input =>
        Integer.parseInt(scala.io.Source.fromFile(input.toFile).mkString)
      }.sum[Int]
      writeTmpFile(s"$sum")
  }

  it should "run a simple scatter gather with the apply method with paths" in {
    val taskManager = getDefaultTaskManager
    val str = "a b c d e f g h i j k l m n o p q r s t u v w x y z ä å ö"
    val input = writeTmpFile(str)

    val simpleScatterGather = ScatterGatherPipeline[Path, Path, Path](
      inDomain = input,
      toSubDomains = fileBasedToIntermediates,
      toOutput = fileBasedToOutput,
      toFinalOutput = fileBasedToFinalOutput
    )
    taskManager.addTask(simpleScatterGather)
    taskManager.runToCompletion(true)
    Integer.parseInt(scala.io.Source.fromFile(simpleScatterGather.gatheredOutput.toFile).mkString) shouldBe 29
  }

  it should "run a merging scatter gather on paths" in {
    val str = "a b c d e f g h i j k l m n o p q r s t u v w x y z ä å ö"
    val input = writeTmpFile(str)

    // have mergeSize be large so that we can test if merging scatter gather reduces to a single gather
    for (mergeSize <- 2 until 29 by 9) {
      val taskManager = getDefaultTaskManager

      val mergingScatterGather = MergingScatterGatherPipeline[Path, Path, Path](
        inDomain = input,
        toSubDomains = fileBasedToIntermediates,
        toOutput = fileBasedToOutput,
        toFinalOutput = fileBasedToFinalOutput
      )
      taskManager.addTask(mergingScatterGather)
      taskManager.runToCompletion(true)
      Integer.parseInt(scala.io.Source.fromFile(mergingScatterGather.gatheredOutput.toFile).mkString) shouldBe 29
    }
  }
}
