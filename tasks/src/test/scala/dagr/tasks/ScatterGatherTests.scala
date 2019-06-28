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
import dagr.core.execsystem.{SystemResources, TaskManager, TaskStatus}
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
  def buildTaskManager: TaskManager = new TaskManager(taskManagerResources = SystemResources.infinite, scriptsDirectory = None, sleepMilliseconds=1)

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

  private case class SplitLineByWord(input: Path) extends SimpleInJvmTask with Partitioner[Path] {
    var partitions: Option[Seq[Path]] = None

    def run(): Unit = {
      val line = {
        val lines = Io.readLines(input).toSeq
        require(lines.size == 1)
        lines.head
      }
      val words = line.split(' ')
      val paths = words.map(_ => tmp())
      words.zip(paths) foreach { case(word, path) => Io.writeLines(path, Seq(word))}
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

  private case class Concat(inputs: Seq[Path], output: Path) extends SimpleInJvmTask {
    def run(): Unit = Io.writeLines(output, inputs.flatMap(Io.readLines))
  }

  private case class ToRange(input: Path) extends SimpleInJvmTask with Partitioner[Int]  {
    var partitions: Option[Seq[Int]] = None
    override def run(): Unit =this.partitions = Some(Range.inclusive(start=1, end=Io.readLines(input).toSeq.head.toInt))
  }

  private case class WriteNumber(number: Int, output: Path) extends SimpleInJvmTask {
    def run(): Unit = Io.writeLines(output, Seq(number.toString))
  }

  "ScatterGather" should "run a simple scatter-gather pipeline on files" in {
    val lines = Seq("one", "one two", "one two three", "one two three four", "one two three four five")
    val lengths = Seq(1,2,3,4,5)

    // setup the input and output
    val input = tmp()
    val sumOfCounts  = tmp()
    val sumOfSquares = tmp()
    Io.writeLines(input, lines)

    val pipeline = new Pipeline() {
      override def build(): Unit = {
        val scatter = Scatter(SplitByLine(input=input))
        val counts = scatter.map(p => CountWords(input=p, output=tmp()))
        val gather = counts.gather(cs => SumNumbers(inputs=cs.map(_.output), output=sumOfCounts))

        val squares = counts.map(c => SquareNumbers(input=c.output, output=tmp()))
        squares.gather(ss => SumNumbers(ss.map(_.output), output=sumOfSquares))

        root ==> scatter
      }
    }

    val taskManager = buildTaskManager
    taskManager.addTask(pipeline)
    taskManager.runToCompletion(true).foreach { case (task, info) =>
      if (TaskStatus.isTaskNotDone(info.status)) {
        println(s"${task.name} $info")
      }
    }

    val sum1 = Io.readLines(sumOfCounts).next().toInt
    val sum2 = Io.readLines(sumOfSquares).next().toInt

    sum1 shouldBe lengths.sum
    sum2 shouldBe lengths.map(x => x*x).sum
  }

  private case class Sample(name: String, library: String, lane: Option[Int]) {
    def toLine: String = s"$name $library" + lane.map(" " + _).getOrElse("")
  }

  private trait SampleTask extends SimpleInJvmTask {
    var _sample: Option[Sample] = None
    def sample: Sample = _sample.get
    def sample_=(s: Sample): Unit = _sample = Some(s)
  }

  private case class ToSample(input: Path) extends SampleTask {
    def run(): Unit = {
      val lines = Io.readLines(input).toSeq
      require(lines.length == 1, s"Could not parse input: '$input'")
      sample = lines.head.split(' ') match {
        case Array(sampleName, library, lane) => Sample(sampleName, library, Some(lane.toInt))
        case _ => throw new IllegalArgumentException(s"Could not parse line: '${lines.head}'")
      }
    }
  }

  private case class MergeLibraries(samples: Seq[Sample]) extends SampleTask {
    def run(): Unit = {
      require(samples.map(l => (l.name, l.library)).distinct.length == 1) // same name+lib
      require(samples.flatMap(_.lane).distinct.length == samples.length) // different lanes
      val library = samples.map(_.library).mkString(":")
      sample = samples.head.copy(library=library, lane=None)
    }
  }

  private case class CountLibraries(samples: Seq[Sample], output: Path) extends SimpleInJvmTask  {
    def run(): Unit = Io.writeLines(output, Seq(samples.map(s => (s.name, s.library)).distinct.length.toString))
  }

  private case class CountSamples(samples: Seq[Sample], output: Path) extends SimpleInJvmTask  {
    def run(): Unit = Io.writeLines(output, Seq(samples.map(_.name).distinct.length.toString))
  }

  it should "run a scatter-gather pipeline with a group-by (1st test case)" in {
    // Three instances of sample 1: two from the same library and a different lane, and a third that is from a different
    // library but the same lane as the first sample.  Then two instances of sample 2 with differing libraries but the
    // same lane.  The goal is to calculate:
    // 1. the # of unique name/library combinations (4: s1.l1, s1.l2, s2.l1, s2.l2)
    // 2. the # of unique samples (2: s1 and s2)
    // 3. the # of "merged libraries" and "merged samples", where samples are merged if they have the same name/library
    //    but different lane.  In this case, s1.l1 is present in two lanes, so we should have four merged libraries. We
    //    should also return only two samples
    val sourceSamples = Seq(
      Sample("s1", "s1.l1", Some(1)),
      Sample("s1", "s1.l1", Some(2)),
      Sample("s1", "s1.l2", Some(1)),
      Sample("s2", "s2.l1", Some(1)),
      Sample("s2", "s2.l2", Some(1))
    )
    val lines = sourceSamples.map(_.toLine)

    // setup the input and output
    val input = tmp()
    val libCounts = tmp()
    val sampleCounts = tmp()
    val mergedLibCounts = tmp()
    val mergedSampleCounts = tmp()

    Io.writeLines(input, lines)

    val pipeline = new Pipeline() {
      override def build(): Unit = {
        // split by line, then map to [[Sample]], then group by sample+library, then merge libraries
        val scatter       = Scatter(SplitByLine(input))
        val samples       = scatter.map { path => ToSample(path) }
        val mergedSamples = samples.groupBy(s => (s.sample.name, s.sample.library))
            .map { case (_, libraries: Seq[ToSample]) => MergeLibraries(libraries.map(_.sample)) }

        // gather counts
        samples.gather(toSamples => CountLibraries(toSamples.map(_.sample), libCounts))
        samples.gather(toSamples => CountSamples(toSamples.map(_.sample), sampleCounts))
        mergedSamples.gather(toSamples => CountLibraries(toSamples.map(_.sample), mergedLibCounts))
        mergedSamples.gather(toSamples => CountSamples(toSamples.map(_.sample), mergedSampleCounts))

        root ==> scatter
      }
    }

    val taskManager = buildTaskManager
    taskManager.addTask(pipeline)
    taskManager.runToCompletion(true)

    val libs          = Io.readLines(libCounts).next().toInt
    val samples       = Io.readLines(sampleCounts).next().toInt
    val mergedLibs    = Io.readLines(mergedLibCounts).next().toInt
    val mergedSamples = Io.readLines(mergedSampleCounts).next().toInt

    libs shouldBe 4
    samples shouldBe 2
    mergedLibs shouldBe 4
    mergedSamples shouldBe 2
  }

  private case class CountTasks(wordCount: Int, tasks: Seq[SimpleInJvmTask], output: Path) extends SimpleInJvmTask  {
    def run(): Unit = Io.writeLines(output, Seq(wordCount + " " + tasks.length))
  }

  private case class GatherWordCounts(tasks: Seq[CountTasks], output: Path) extends SimpleInJvmTask  {
    def run(): Unit = Io.writeLines(output, tasks.flatMap { task => Io.readLines(task.output) })
  }

  it should "run a scatter-gather pipeline with a group-by (2nd test case)" in {
    // This example will:
    // 1. take a single file, split by line
    // 2. for each line, count the # of words (using spaces)
    // 3. group by per-line word count, return how many lines how a given # of words
    // The result should be:
    // | # of words | count |
    // |    ---     |  ---  |
    // |     1      |   3   |
    // |     2      |   2   |
    // |     5      |   1   |
    // |     9      |   1   |

    val lines = Seq(
      "this is the first line",
      "second",
      "third line",
      "fourth line",
      "this is a really, really, really, long fifth line",
      "sixth",
      "seventh"
    )

    val input = tmp()
    val totalWordCountOutput = tmp()
    val wordCountFreqOutput = tmp()

    Io.writeLines(input, lines)

    val pipeline = new Pipeline() {
      override def build(): Unit = {
        // split the input by line
        val scatterByLine= Scatter(SplitByLine(input))
        // for each line, count the # of words in that line
        val scatterByLineWordCount = scatterByLine.map { p => CountWords(input=p, output=tmp()) }
        // sum the per-line word counts
        scatterByLineWordCount.gather { byLineWordCounts =>
          SumNumbers(byLineWordCounts.map(_.output), output=totalWordCountOutput)
        }
        // 1. group the per-line word counts by their value
        // 2. for each unique per-line word count, count the # of lines with that value
        // 3. gather the results
        scatterByLineWordCount.groupBy { byLineWordCount =>
          Io.readLines(byLineWordCount.output).next().toInt
        }.map {
          case (wordCount, tasks) => CountTasks(wordCount, tasks, tmp())
        }.gather {
          countTasks => GatherWordCounts(countTasks, wordCountFreqOutput)
        }

        root ==> scatterByLine
      }
    }

    val taskManager = buildTaskManager
    taskManager.addTask(pipeline)
    taskManager.runToCompletion(true)

    val totalWordCount = Io.readLines(totalWordCountOutput).next().toInt
    val wordCountFreq  = Io.readLines(wordCountFreqOutput).map { line =>
      line.split(' ') match {
        case Array(wordCount, occurrence) => (wordCount.toInt, occurrence.toInt)
      }
    }.toMap

    totalWordCount shouldBe 21
    wordCountFreq.toSeq.sortBy(_._1) should contain theSameElementsInOrderAs Seq((1, 3), (2, 2), (5, 1), (9, 1))
  }

  it should "scatter than flatten and gather" in {
    val lines = Seq("one", "one two", "one two three", "one two three four", "one two three four five")
    val lengths = Seq(1,2,3,4,5)

    // setup the input and output
    val input = tmp()
    val sumOfCountsFlatMap = tmp()
    Io.writeLines(input, lines)

    val pipeline = new Pipeline() {
      override def build(): Unit = {
        // the initial scatter
        val scatter: Scatter[Path] = Scatter(SplitByLine(input=input))

        // flatMap
        val scatterByWordFlatMap: Scatter[Path] = scatter.flatMap { pathToLine => Scatter(SplitLineByWord(pathToLine)) }
        val countByWords = scatterByWordFlatMap.map { pathToWord: Path => CountWords(input=pathToWord, output=tmp()) }
        countByWords.gather(cs => SumNumbers(inputs=cs.map(_.output), output=sumOfCountsFlatMap))

        root ==> scatter
      }
    }

    val taskManager = buildTaskManager
    taskManager.addTask(pipeline)
    taskManager.runToCompletion(true)

    val sumFlatMap = Io.readLines(sumOfCountsFlatMap).next().toInt

    sumFlatMap shouldBe lengths.sum
  }
}
