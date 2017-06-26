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

import java.text.DecimalFormat

import _root_.picard.analysis.directed.HsMetrics
import dagr.core.cmdline.Pipelines
import com.fulcrumgenomics.sopt._
import dagr.tasks.DagrDef
import DagrDef._
import dagr.core.tasksystem.{Linker, NoOpInJvmTask, Pipeline, SimpleInJvmTask}
import com.fulcrumgenomics.commons.io.Io
import dagr.api.models.{Cores, Memory, ResourceSet}
import dagr.tasks.jeanluc.FilterBam
import dagr.tasks.picard.{CollectHsMetrics, DownsampleSam, DownsamplingStrategy}
import htsjdk.samtools.metrics.MetricsFile

import scala.collection.JavaConversions._

/**
  * Pipeline to downsample a Tumor to a specific median coverage level (per CollectHsMetrics)
  * and run somatic variant calling on it.
  */
@clp(
  description =
    """
      |Downsamples a Tumor BAM to various coverage levels and calls mutations in it." +
    """,
  group = classOf[Pipelines]
)
class DownsampleAndCallSomaticVariants
( @arg(flag='t', doc="Tumor BAM file")                          val tumorBam:  PathToBam,
  @arg(flag='n', doc="Normal BAM file")                         val normalBam: PathToBam,
  @arg(flag='r', doc="Reference FASTA file")                    val ref: PathToFasta,
  @arg(flag='l', doc="Regions to call over")                    val intervals: PathToIntervals,
  @arg(          doc="One or more coverage levels to call at.") val coverage:  Seq[Int],
  @arg(flag='o', doc="Output directory")                        val output:    DirPath)
  extends Pipeline(Some(output)) {

  override def build(): Unit = {
    val tmp = output.resolve("tmp")
    Io.mkdirs(tmp)

    // Paths that will be used in the pipeline
    val filteredTumor  = tmp.resolve("tumor.bam")
    val filteredNormal = tmp.resolve("normal.bam")

    val filterTumor  = new FilterBam(tumorBam,  filteredTumor,  Some(intervals))
    val filterNormal = new FilterBam(normalBam, filteredNormal, Some(intervals))
    val hsMetrics    = new CollectHsMetrics(in=filteredTumor, ref=ref, targets=intervals)
    val fetchMedian  = new FetchMedianCoverage(hsMetrics.metricsFile)

    root ==> (filterTumor :: filterNormal)
    filterTumor ==> hsMetrics ==> fetchMedian

    // Build a calling workflow for each coverage level
    val callers = coverage.foreach(cov => {
      val prefix = output.resolve(pad(cov))
      val call = new DsAndCallOnce(tumorBam=filteredTumor, normalBam=filteredNormal, ref=ref, intervals=intervals, outputPrefix=prefix)
      fetchMedian ==> Linker(fetchMedian, call)((f,c) => f.medianCoverage.foreach(m => c.downsamplingP = cov / m.toDouble)) ==> call
    })
  }

  /** Pads out a coverage number to a 3-digit String, and adds X on the end. */
  def pad(i : Int): String = new DecimalFormat("000").format(i) + "X"
}

/**
  * Fairly simple inner pipeline that will either do nothing, or downsample and call if
  * downsampling is possible.
  */
private class DsAndCallOnce(val tumorBam:  PathToBam,
                            val normalBam: PathToBam,
                            val ref: PathToFasta,
                            val intervals: PathToIntervals,
                            val outputPrefix: DirPath,
                            var downsamplingP: Double = 1) extends Pipeline {

  override def build(): Unit = {
    if (downsamplingP > 1) {
      logger.warning("Not downsampling " + tumorBam + " because P=" + downsamplingP)
      root ==> new NoOpInJvmTask("NoOp")
    }
    else {
      val strat = Some(DownsamplingStrategy.HighAccuracy)
      val dsBam = outputPrefix.getParent.resolve(outputPrefix.getFileName.toString + ".tumor.bam")

      val downsample = new DownsampleSam(in=tumorBam, out=dsBam, proportion=downsamplingP, strategy=strat).requires(Cores(1), Memory("32g"))
      val callOnce = new SomaticVariantCallingPipeline(tumorBam=dsBam, normalBam=normalBam, ref=ref, intervals=intervals, outputPrefix=outputPrefix)
      root ==> downsample ==> callOnce
    }
  }
}

/** Pops open the HS metrics file and reads out the median coverage. */
private class FetchMedianCoverage(hsMetrics: FilePath) extends SimpleInJvmTask {
  var medianCoverage : Option[Int] = None

  override def run(): Unit = {
    val mfile = new MetricsFile[HsMetrics,java.lang.Integer]
    mfile.read(Io.toReader(hsMetrics))
    val median = mfile.getMetrics.map(hs => hs.MEDIAN_TARGET_COVERAGE).max
    this.medianCoverage = Some(median.toInt)
  }
}
