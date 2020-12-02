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
package dagr.pipelines

import java.nio.file.{Files, Path}

import dagr.commons.io.Io
import dagr.core.cmdline.Pipelines
import dagr.core.config.Configuration
import dagr.core.execsystem.{Cores, Memory}
import dagr.core.tasksystem.{EitherTask, Linker, NoOpInJvmTask, NoOpTask, Pipeline, SimpleInJvmTask}
import dagr.sopt.{arg, clp}
import dagr.tasks.DagrDef.{DirPath, FilePath, PathToBam, PathToFasta, PathToFastq, PathToIntervals}
import dagr.tasks.bwa.BwaMem
import dagr.tasks.misc.{LinkFile, MoveFile}
import dagr.tasks.picard.{CollectHsMetrics, CollectMultipleMetrics, DeleteBam, DownsampleSam, DownsamplingStrategy, MarkDuplicates, SortSam}
import htsjdk.samtools.SAMFileHeader.SortOrder
import htsjdk.samtools.metrics.MetricsFile
import picard.analysis.CollectMultipleMetrics.Program
import picard.analysis.directed.HsMetrics

import scala.collection.mutable

/**
  * Downsample Bams to create composite contaminated bams,
  * then downsample to various depths
  * and run VBID with different number of target snps to evaluate
  * dependance of VBID on contamination & depth & number of target snps
  */
@clp(description = "Example FASTQ to BAM pipeline.", group = classOf[Pipelines])
class ContaminateAndEvaluateVBID
(@arg(flag = "i", doc = "Input bams.") val bams: Seq[PathToBam],
 @arg(flag = "r", doc = "Reference fasta.") val ref: PathToFasta,
 @arg(flag = "o", doc = "Output directory.") val out: DirPath,
 @arg(flag = "t", doc = "Target regions.") val targets: PathToIntervals,
 @arg(flag = "p", doc = "Output file prefix.") val prefix: String,
 @arg(flag = "c", doc = "Contamination levels to evaluate.") val contaminations: Seq[Double],
 @arg(flag = "d", doc = "Depth values to evaluate.") val depths: Seq[Integer],
 @arg(flag = "m", doc = "Number of markers to use.") val markers: Seq[Integer]

) extends Pipeline(Some(out)) {

  override def build(): Unit = {
    val strat = Some(DownsamplingStrategy.HighAccuracy)

    val bam = out.resolve(prefix + ".bam")
    val tmpBam = out.resolve(prefix + ".tmp.bam")
    val metricsPrefix: Some[DirPath] = Some(out.resolve(prefix))
    Files.createDirectories(out)
    val bamYield = new mutable.HashMap[PathToBam, Integer]

    // Collect the median target coverage in each of the inputs and put result in a map
    val yieldsCollected = new NoOpTask()
    root ==> yieldsCollected

    bams.map(bam => {
      val prefixPath: Path = out.resolve(prefix + bam.getFileName)
      val metricPath: Path = out.resolve(prefix + bam.getFileName + ".qualityYieldMetrics")
      val hsMetrics = new CollectHsMetrics(in = bam, ref = ref, targets = targets)

      val collectYield = new CollectMultipleMetrics(in = bam, prefix = Some(prefixPath),
        ref = ref, programs = Seq(Program.CollectQualityYieldMetrics))
      val fetchMedian = new FetchMedianCoverage(metricPath)
      root ==> hsMetrics ==> fetchMedian ==> SimpleInJvmTask(bamYield.put(bam, fetchMedian.medianCoverage.getOrElse(0))) ==> yieldsCollected
    })

    val pairsOfBam = for (x <- bams; y <- bams) yield (x, y)

    pairsOfBam.foreach { case (x, y) => {
      contaminations.foreach(c => {
        // create a mixture of c x + (1-c) y taking into account the depths of
        // x and y
        // D_x and D_y are the effective depths of x and y
        // if we are to downsample x we need x to be downsampled to c/(1-c)
        // if we are to downsample y we need y to be downsampled to (1-c)/c
        // of of those two values will be less than 1.
        val downsampleX = new DownsampleSam(in = x, out = dsX, proportion = c / (1 - c), strategy = strat).requires(Cores(1), Memory("32g"))
        val downsampleY = new DownsampleSam(in = x, out = dsY, proportion = (1 - c) / c, strategy = strat).requires(Cores(1), Memory("32g"))

        val copyX = new LinkFile(x, dsX)
        val copyY = new LinkFile(y, dsY)

        val downsample = EitherTask.of(downsampleX, downsampleY, c / (1 - c) <= 1) ::
          EitherTask.of(copyY, copyX, c / (1 - c) <= 1)

      })
    }
    }

    depths.foreach(depth => {
      con


    })
    val bwa = new BwaMem(fastq = fastq, ref = ref)
    val sort = new SortSam(in = Io.StdIn, out = tmpBam, sortOrder = SortOrder.coordinate) with Configuration {
      requires(Cores(2), Memory("2G"))
    }
    val mark = new MarkDuplicates(inputs = Seq(tmpBam), out = Some(bam), prefix = metricsPrefix) with Configuration {
      requires(Cores(1), Memory("2G"))
    }
    val rmtmp = new DeleteBam(tmpBam)

    root ==> (bwa | sort) ==> mark ==> rmtmp
    targets.foreach(path => root ==> new CollectHsMetrics(in = bam, prefix = metricsPrefix, targets = path, ref = ref))
  }

}

/** Pops open the HS metrics file and reads out the median coverage. */
private class FetchMedianCoverage(hsMetrics: FilePath) extends SimpleInJvmTask {
  var medianCoverage: Option[Int] = None

  override def run(): Unit = {
    val mfile = new MetricsFile[HsMetrics, java.lang.Integer]
    mfile.read(Io.toReader(hsMetrics))
    val median = mfile.getMetrics.map(hs => hs.MEDIAN_TARGET_COVERAGE).max
    this.medianCoverage = Some(median.toInt)
  }
}


