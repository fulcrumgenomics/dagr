/*
 * The MIT License
 *
 * Copyright (c) 2015-2016 Fulcrum Genomics LLC
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

import dagr.core.cmdline._
import dagr.core.tasksystem.{Callbacks, Pipeline, ProcessTask, Task}
import dagr.core.util.{Io, PathUtil}
import dagr.tasks._
import dagr.tasks.bwa.{Bwa, BwaBacktrack}
import dagr.tasks.misc.CalculateDownsamplingProportion
import dagr.tasks.picard._

import scala.collection.mutable.ListBuffer

object DnaResequencingFromUnmappedBamPipeline {
  final val SUMMARY =
    """
      |Dna Resequencing From Unmapped BAM Pipeline.  Runs:
      |  - Unmapped BAM -> EstimateLibraryComplexity"
      |  - Unmapped BAM -> SamToFastq -> Bwa Mem -> MergeBamAlignment -> MarkDuplicates -> Mapped BAM
      |  - Mapped BAM -> {CollectMultipleMetrics, EstimateLibraryComplexity, ValidateSamFile}
      |  - Mapped BAM -> {CalculateHsMetrics, CollectTargetedPcrMetrics} if targets are given
      |  - Mapped BAM -> {CollectWgsMetrics, CollectGcBiasMetrics} if targets are not given
      |"""
}

@CLP(
  description = DnaResequencingFromUnmappedBamPipeline.SUMMARY,
  group = classOf[Pipelines])
class DnaResequencingFromUnmappedBamPipeline(
  @Arg(doc="Path to the unmapped BAM.")                            val unmappedBam: PathToBam,
  @Arg(doc="Path to the reference FASTA.")                         val referenceFasta: PathToFasta,
  @Arg(doc="Use bwa aln/sampe' instead of bwa mem.")               val useBwaBacktrack: Boolean = false,
  @Arg(doc="The number of reads to target when downsampling.")     val downsampleToReads: Long = Math.round(185e6 / 101),
  @Arg(flag="t", doc="Target intervals to run HsMetrics over.")    val targetIntervals: Option[PathToIntervals],
  @Arg(doc="Path to a temporary directory.")                       val tmp: Path,
  @Arg(flag="o", doc="The output directory to write to.")          val output: DirPath,
  @Arg(doc="The filename prefix for output files.")                val basename: FilenamePrefix
) extends Pipeline(Some(output)) {

  name = getClass.getSimpleName

  override def build(): Unit = {
    val prefix = output.resolve(basename)

    Io.assertReadable(referenceFasta)
    if (targetIntervals.isDefined) Io.assertReadable(targetIntervals.get)
    Io.assertCanWriteFile(prefix, parentMustExist=false)
    Files.createDirectories(output)

    val mappedBam   = Files.createTempFile(tmp, "mapped.", ".bam")
    val dsMappedBam = Files.createTempFile(tmp, "mapped.ds.", ".bam")
    val finalBam    = PathUtil.pathTo(prefix + ".bam")
    val dsPrefix    = PathUtil.pathTo(prefix + ".ds")
    val dsFinalBam  = PathUtil.pathTo(dsPrefix + ".bam")

    val fastqToBams = ListBuffer[ProcessTask]()

    ///////////////////////////////////////////////////////////////////////
    // Run BWA mem, then do some downsampling
    ///////////////////////////////////////////////////////////////////////
    val bwa = if (useBwaBacktrack) {
      new BwaBacktrack(unmappedBam=unmappedBam, mappedBam=mappedBam, ref=referenceFasta)
    }
    else {
      Bwa.bwaMemStreamed(unmappedBam=unmappedBam, mappedBam=mappedBam, ref=referenceFasta)
    }

    root ==> bwa

    // Just collect quality yield metrics on the pre-de-duped BAM as we need this for downsampling
    val yieldMetrics        = new CollectMultipleMetrics(
      in=mappedBam,
      prefix=Some(prefix),
      programs=List(MetricsProgram.CollectQualityYieldMetrics),
      ref=referenceFasta
    )
    val calculateProportion = new CalculateDownsamplingProportion(PathUtil.pathTo(prefix + ".quality_yield_metrics.txt"), downsampleToReads)
    val downsampleSam       = new DownsampleSam(in=mappedBam, out=dsMappedBam, proportion=1, accuracy=Some(0.00001))

    Callbacks.connect(downsampleSam, calculateProportion)((ds, cp) => ds.proportion=cp.proportion)
    bwa ==> yieldMetrics ==> calculateProportion ==> downsampleSam

    ///////////////////////////////////////////////////////////////////////
    // Do all the downstream steps for both the Full and Downsampled BAM
    ///////////////////////////////////////////////////////////////////////
    List((mappedBam, finalBam, prefix, "(Full)", bwa), (dsMappedBam, dsFinalBam, dsPrefix, "(DS)", downsampleSam)).foreach(
      (tuple: (Path,Path,Path,String,Task)) => {
        val (mapped, deduped, pre, note, prevTask) = tuple

        ///////////////////////////////////////////////////////////////////////
        // Mark Duplicates and then cleanup the mapped bam
        ///////////////////////////////////////////////////////////////////////
        val markDuplicates: MarkDuplicates = new MarkDuplicates(in = mapped, out = Some(deduped))
        prevTask ==> markDuplicates
        (markDuplicates :: downsampleSam) ==> new RemoveBam(mapped)

        ///////////////////////////////////////////////////////////////////////
        // Do either HS metrics or WGS metrics, but not both
        ///////////////////////////////////////////////////////////////////////
        if (targetIntervals.isDefined) {
          markDuplicates ==> new CollectHsMetrics(in=deduped, prefix=Some(pre), ref=referenceFasta, targets=targetIntervals.get)
        }
        else {
          markDuplicates ==> new CollectWgsMetrics(in=deduped, prefix=Some(pre), ref=referenceFasta)
          markDuplicates ==> new CollectGcBiasMetrics(in=deduped, prefix=Some(pre), ref=referenceFasta)
        }

        ///////////////////////////////////////////////////////////////////////
        // Metrics that apply to both WGS and HS
        ///////////////////////////////////////////////////////////////////////
        markDuplicates ==> new EstimateLibraryComplexity(in=deduped, prefix=Some(pre))
        markDuplicates ==> new CollectMultipleMetrics(in=deduped, prefix=Some(pre), ref=referenceFasta)
        markDuplicates ==> new ValidateSamFile(in=deduped, prefix=Some(pre), ref=referenceFasta)
      })
  }
}
