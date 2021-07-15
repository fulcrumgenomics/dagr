/*
 * The MIT License
 *
 * Copyright (c) 2020 Fulcrum Genomics
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

package dagr.pipelines

import java.nio.file.Path
import java.time.{Duration, Instant}

import com.fulcrumgenomics.commons.CommonsDef.javaIterableToIterator
import com.fulcrumgenomics.commons.io.Io
import com.fulcrumgenomics.commons.reflect.{ReflectionUtil, ReflectiveBuilder}
import com.fulcrumgenomics.commons.util.{LazyLogging, NumericCounter, TimeUtil}
import com.fulcrumgenomics.sopt.{arg, clp}
import dagr.core.DagrDef.TaskId
import dagr.core.cmdline.Pipelines
import dagr.core.execsystem.{Cores, Memory, ResourceSet, TaskInfo}
import dagr.core.tasksystem.{Pipeline, SimpleInJvmTask}
import dagr.pipelines.DagrReportPipeline._
import dagr.pipelines.DagrReportTask.MergeFiles
import dagr.tasks.DagrDef.DirPath
import dagr.tasks.ScatterGather.Scatter
import htsjdk.samtools.util.{Interval, OverlapDetector}

import scala.collection.mutable
import scala.reflect.runtime.{universe => ru}

@clp(description=
  """
    |Analyzes dagr-produced execution reports.
    |
    |This pipeline will, in parallel, collate task execution information from one or more dagr-produced execution
    |reports.
    |
    |The log files for the accompanying dagr execution should be in the same directory as listed **within** report, or
    |be in a "logs" directory in the same directory **as** the report.
    |
    |Two outputs will be created.  First, a file named "execution_stats.by_task.tab", which collates statistics about
    |each type of task.  For example, the min/max/mean/median cores reserved by a given task. Resources (ex. memory,
    |cores) are examined, as well as the various durations (ex. execution time, time from submission).  The second
    |output is named "execution_stats.by_time.tab", and this gives information about the state of the pipeline at every
    |second of execution.  For example, what tasks are eligible to run, what tasks are running, the total amount of
    |cores or memory reserved.
    |""",

  group = classOf[Pipelines])
class DagrReportPipeline
( @arg(flag='r', doc="The dagr-produced execution report(s).") val reports: Seq[PathToReport],
  @arg(doc="The output directory") val outputDir: DirPath,
  @arg(doc = "Set the number of cores available to dagr.")
  val cores: Cores,
  @arg(doc = "Set the memory available to dagr.")
  val memory: Memory,
  @arg(doc="Include pipelines in the output.  Logs must exist or be located in a \"logs\" directory that is in the same directory as the report")
  val pipelines: Boolean = false
) extends Pipeline with LazyLogging {

  Io.assertReadable(reports)
  Io.assertWritableDirectory(outputDir.getParent)

  /** Finds the longest common prefix. */
  private def commonPrefix(strs: Seq[String]): String = {
    strs.head.zipWithIndex.takeWhile { case (ch, i) => strs.forall(_(i) == ch) }.map(_._1).mkString
  }

  override def build(): Unit = {
    // Make the reports human-readable by removing the longest common prefix and suffix.
    val (prefix, suffix) = if (reports.length == 1) (reports.head.toString, "") else {
      (commonPrefix(reports.map(_.toString)), commonPrefix(reports.map(_.toString.reverse)).reverse)
    }
    def reportName(report: Path): String = {
      val str = report.toString
      val toReturn = str.slice(from=prefix.length, until=str.length - suffix.length).replace("/", ".")
      if (toReturn.isEmpty) "NA" else toReturn
    }

    // Process each report independently
    val reportTaskOutputDir = if (this.reports.length == 1) Some(outputDir) else None
    val reportTasks = this.reports.map { report =>
      new DagrReportTask(
        report     = report,
        reportName = reportName(report),
        cores      = this.cores,
        memory     = this.memory,
        pipelines  = this.pipelines,
        outputDir  = reportTaskOutputDir
      )
    }

    // If only one report, output directly to the output directory, otherwise, scatter gather
    if (this.reports.size == 1) reportTasks.foreach(root ==> _) else {
      // merge them (could copy if we have one file)
      val mergeByTaskPaths = new MergeFiles(
        paths  = reportTasks.map(_.byTaskPath),
        output = DagrReportTask.toByTaskPath(outputDir)
      ) withName "MergeByTaskPaths"
      val mergeByTimePaths = new MergeFiles(
        paths  = reportTasks.map(_.byTimePath),
        output = DagrReportTask.toByTimePath(outputDir)
      ) withName "MergeByTimePaths"

      // wire it all together
      reportTasks.foreach { reportTask =>
        root ==> reportTask ==> (mergeByTaskPaths :: mergeByTimePaths)
      }
    }
  }
}

object DagrReportPipeline {
  type PathToReport = Path

  /** Returns all the [[TaskInfo]]s from a given report. */
  def from(report: Path): Iterator[TaskInfo] = {
    val logsDir = report.getParent.resolve("logs")
    TaskInfo.from(report=report, logsDir=Some(logsDir))
  }

  /** The type of resource or statistic that's rolled up in a by-task report. */
  object ResourceType {
    sealed case class ResourceTypeValue(name: String, unit: String, toValue: TaskInfo => Double)
    val Cores: ResourceTypeValue            = ResourceTypeValue("cores", "cores", _.cores.value)
    val Memory: ResourceTypeValue           = ResourceTypeValue("memory", "gb", _.memory.value / (1024*1024*1024))
    val EligibleSeconds: ResourceTypeValue  = ResourceTypeValue("eligible-seconds", "seconds", t => instantDiff(t.submissionDate, t.startDate))
    val WallClockSeconds: ResourceTypeValue = ResourceTypeValue("wall-clock-seconds", "seconds", t => instantDiff(t.submissionDate, t.endDate))
    val ExecutionSeconds: ResourceTypeValue = ResourceTypeValue("execution-seconds", "seconds", t => instantDiff(t.startDate, t.endDate))
    val CoreSeconds: ResourceTypeValue      = ResourceTypeValue("core-seconds", "seconds", t => instantDiff(t.startDate, t.endDate) * t.cores.value)
    def values: Iterator[ResourceTypeValue] = Iterator(Cores, Memory, EligibleSeconds, WallClockSeconds, ExecutionSeconds, CoreSeconds)
    private def instantDiff(start: Instant, end: Instant): Double = Duration.between(start, end).getSeconds
  }

  /** The statistics for all tasks with a common name and a single type of resource */
  case class TaskGroupStats
  (name: String,
   resource: String,
   unit: String,
   count: Long,
   min: Double,
   max: Double,
   mean: Double,
   median: Double,
   stddev: Double,
   ids: Seq[TaskId],
   values: Seq[Double]
  ) {

    /** Get the values of the arguments in the order they were defined. */
    def strings: Iterator[String] = productIterator.map(formatValue).toIterator

    /** Override this method to customize how values are formatted. */
    protected def formatValue(value: Any): String = value match {
      case null           => ""
      case None           => ""
      case Some(x)        => formatValue(x)
      case seq: Seq[_]    => seq.map(formatValue).mkString(",")
      case f: Long        => formatValue(f.toDouble)
      case d: Double if d.isNaN || d.isInfinity => d.toString
      case d: Double      => f"$d%.2f"
      case other          => other.toString
    }
  }

  object TaskGroupStats {TaskInfo
    /** Builds the [[TaskGroupStats]]s from a set of tasks */
    def build(tasks: Seq[TaskInfo]): Iterator[TaskGroupStats] = {
      val name = tasks.head.name
      // foreach resource types
      ResourceType.values.map { summaryType =>
        val counter = new NumericCounter[Double]()
        val values  = tasks.map(summaryType.toValue)
        values.foreach(counter.count)
        val mean    = counter.mean()
        new TaskGroupStats(
          name     = name,
          resource = summaryType.name,
          unit     = summaryType.unit,
          count    = counter.total,
          min      = counter.map(_._1).min,
          max      = counter.map(_._1).max,
          mean     = mean,
          median   = counter.median(),
          stddev   = counter.stddev(m=mean),
          ids      = tasks.map(_.id),
          values   = values
        )
      }
    }

    /** Get the names of the arguments in the order they were defined for the type [T]. */
    def names(implicit tt: ru.TypeTag[TaskGroupStats]): Iterator[String] = {
      val clazz             = ReflectionUtil.typeTagToClass[TaskGroupStats]
      val reflectiveBuilder = new ReflectiveBuilder(clazz)
      reflectiveBuilder.argumentLookup.ordered.iterator.map(_.name)
    }
  }
}


/** A task that parses a single execution report.  Outputs are stored on disk. */
class DagrReportTask(report: PathToReport,
                     reportName: String,
                     cores: Cores,
                     memory: Memory,
                     pipelines: Boolean,
                     outputDir: Option[DirPath] = None) extends SimpleInJvmTask with LazyLogging {
  requires(Cores(1), Memory("1g"))
  this.name = this.name + "." + reportName
  private val _outputDir: DirPath = outputDir.getOrElse(Io.makeTempDir(this.name))
  val byTaskPath: Path = DagrReportTask.toByTaskPath(_outputDir)
  val byTimePath: Path = DagrReportTask.toByTimePath(_outputDir)

  override def run(): Unit = {
    _outputDir.toFile.mkdirs()

    // get the tasks
    val reportTasks = from(report).filter(task => pipelines || !task.isPipeline).toSeq

    logger.info("Pre-loading caches to format values as strings")
    // Pre-load the caches.  These optimizations based on profiling can speed up string generation and writing by 10x.
    val taskIdToStringCache = new scala.collection.mutable.HashMap[TaskId, String]()
    def toTaskIdString(taskId: TaskId): String = taskIdToStringCache.getOrElse(taskId, {
      val str = taskId.toString()
      taskIdToStringCache.put(taskId, str)
      str
    })
    val coresToStringCache = new scala.collection.mutable.HashMap[Cores, String]()
    def toCoresString(cores: Cores): String = coresToStringCache.getOrElse(cores, {
      val str = f"${cores.value}%.2f"
      coresToStringCache.put(cores, str)
      str
    })
    val memoryToStringCache = new scala.collection.mutable.HashMap[Memory, String]()
    def toMemoryString(memory: Memory): String = memoryToStringCache.getOrElse(memory, {
      val str = memory.gb
      memoryToStringCache.put(memory, str)
      str
    })
    val taskInfoToStringCache = new mutable.HashMap[TaskInfo, String]()
    def toTaskInfoString(task: TaskInfo): String = taskInfoToStringCache.getOrElse(task, {
      val str = f"${toTaskIdString(task.id)};${task.name};${toCoresString(task.cores)};${toMemoryString(task.memory)}"
      taskInfoToStringCache.put(task, str)
      str
    })
    // populate the caches
    reportTasks.foreach { task => toTaskInfoString(task) }

    // Build and output the by-task report
    // Group tasks by name, and output some useful statistics about resource usage
    {
      logger.info("Grouping tasks and computing stats")
      val writer = Io.toWriter(byTaskPath)
      writer.write("report_path\t" + TaskGroupStats.names.mkString("\t") + "\n")
      reportTasks
        .map(task => task.copy(name=task.name.split('.').head))
        .groupBy(_.name)
        .toSeq.sortBy(_._1).iterator
        .flatMap { case (_, tasksForName) => TaskGroupStats.build(tasks = tasksForName) }
        .foreach { taskGroupInfo =>
          writer.write(reportName)
          taskGroupInfo.strings.foreach { value => writer.write("\t" + value)}
          writer.write('\n')
        }
      writer.close()
    }

    // Build and output the by-time report
    logger.info("Outputting tasks by time")
    val writer = Io.toWriter(byTimePath)
    val header = Seq("report_path", "date", "seconds_offset",
      "reserved_cores", "reserved_memory_gb", "unreserved_cores", "unreserved_memory_gb",
      "num_eligible", "eligible_info", "num_executing", "executing_info",
      "num_completed", "completed_info", "num_could_have_run", "could_have_run_info"
    )
    writer.write(header.mkString("\t") + "\n")
    logger.info(s"Outputting tasks by time for report: $report")
    val tasks             = reportTasks
    require(tasks.nonEmpty, s"Found no tasks for report: $report")
    val pipelineStartDate = tasks.iterator.map(_.submissionDate).min
    val pipelineEndDate   = tasks.iterator.map(_.endDate).max.plusSeconds(1)

    // Get a map from time in seconds to the tasks that are eligible
    val eligibleDetector = new OverlapDetector[TaskInfo](0, 0)
    tasks.foreach { task =>
      val offset = Duration.between(pipelineStartDate, task.submissionDate).getSeconds.toInt
      val length = Duration.between(task.submissionDate, task.startDate).getSeconds.toInt
      val interval = new Interval("1", offset, offset + length)
      eligibleDetector.addLhs(task, interval)
    }

    // Get a map from time in seconds to the tasks that are executing
    val executionDetector = new OverlapDetector[TaskInfo](0, 0)
    tasks.foreach { task =>
      val offset = Duration.between(pipelineStartDate, task.startDate).getSeconds.toInt
      val length = Duration.between(task.startDate, task.endDate).getSeconds.toInt
      val interval = new Interval("1", offset, offset + length)
      eligibleDetector.addLhs(task, interval)
    }

    // Get a map from time in seconds to the tasks that completed
    val completedMap: Map[BigDecimal, Seq[TaskInfo]] = tasks.map { task =>
      (BigDecimal(Duration.between(pipelineStartDate, task.endDate).getSeconds), task)
    }.groupBy(_._1).map { case (seconds, values) => (seconds, values.map(_._2)) }

    // Now do the actual work
    val pipelineEndSeconds = Duration.between(pipelineStartDate, pipelineEndDate).getSeconds
    var curSecond = 0
    // TODO: cumulative stats?
    while (curSecond <= pipelineEndSeconds) {
      if (curSecond % 1000 == 0) {
        logger.info(f"On ${curSecond + 1}%,d / $pipelineEndSeconds%,d (${100.0 * curSecond / pipelineEndSeconds.toDouble}%.1f%%)")
      }
      // get all the necessary data
      val completed        = completedMap.getOrElse(curSecond, Seq.empty).sortBy(_.id)
      val eligible         = eligibleDetector.getOverlaps(new Interval("1", curSecond, curSecond)).toSeq.sortBy(_.id)
      val executing        = executionDetector.getOverlaps(new Interval("1", curSecond, curSecond)).toSeq.sortBy(_.id)
      val reservedCores    = executing.map(_.cores).foldLeft(Cores.none)((a,b) => a + b)
      val reservedMemory   = executing.map(_.memory).foldLeft(Memory.none)((a,b) => a + b)
      val numEligible      = eligible.length
      val numExecuting     = executing.length
      val numCompleted     = completed.length
      val unreservedCores  = if (this.cores > reservedCores) this.cores - reservedCores else Cores.none
      val unreservedMemory = if (this.memory > reservedMemory) this.memory - reservedMemory else Memory.none
      val unreserved       = ResourceSet(unreservedCores, unreservedMemory)

      // examine the eligible tasks to see if any could be run with resources not reserved
      val couldHaveExecuted = eligible.filter { task => unreserved.subset(task.cores, task.memory).isDefined }
      val numCouldHaveExecuted = couldHaveExecuted.length

      // method to write the task infos
      def writeTasks(tasks: Seq[TaskInfo]): Unit = {
        var first = true
        tasks.foreach { task =>
          if (!first) writer.write(',')
          writer.write(toTaskInfoString(task))
          first = false
        }
      }

      // Basic info
      writer.write(reportName)
      writer.write('\t')
      writer.write(TimeUtil.timestampStringOrNA(Some(pipelineStartDate.plusSeconds(curSecond))))
      writer.write('\t')
      writer.write(curSecond.toString)
      writer.write('\t')
      writer.write(toCoresString(reservedCores))
      writer.write('\t')
      writer.write(toMemoryString(reservedMemory))
      writer.write('\t')
      writer.write(toCoresString(unreservedCores))
      writer.write('\t')
      writer.write(toMemoryString(unreservedMemory))
      // eligible tasks
      writer.write('\t')
      writer.write(numEligible.toString)
      writer.write('\t')
      writeTasks(eligible)
      // executing
      writer.write('\t')
      writer.write(numExecuting.toString)
      writer.write('\t')
      writeTasks(executing)
      // completed
      writer.write('\t')
      writer.write(numCompleted.toString)
      writer.write('\t')
      writeTasks(completed)
      // could have run
      writer.write('\t')
      writer.write(numCouldHaveExecuted.toString)
      writer.write('\t')
      writeTasks(couldHaveExecuted)
      // newline!
      writer.write('\n')

      curSecond += 1
    }
    writer.close()

    logger.info(f"All done ")
  }
}

object DagrReportTask {
  /** The path to the by-task report. */
  def toByTaskPath(outputDir: DirPath): Path = outputDir.resolve("execution_stats.by_task.tab")
  /** The path to the by-time report. */
  def toByTimePath(outputDir: DirPath): Path = outputDir.resolve("execution_stats.by_time.tab")

  /** A task that merges multiple line-oriented files with a common header line */
  class MergeFiles(paths: Seq[Path], output: Path) extends SimpleInJvmTask {
    def run(): Unit = {
      val writer = Io.toWriter(output)
      val header = Io.readLines(paths.head).next()
      writer.write(header + '\n')
      paths.foreach { path =>
        Io.readLines(path).drop(1).foreach { line =>
          writer.write(line)
          writer.write('\n')
        }
      }
      writer.close()
    }
  }
}