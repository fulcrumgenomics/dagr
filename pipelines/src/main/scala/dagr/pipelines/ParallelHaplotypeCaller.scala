/*
 * The MIT License
 *
 * Copyright (c) 2016 Fulcrum Genomics
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

import java.nio.file.{Files, Path}

import dagr.core.cmdline.Pipelines
import dagr.core.tasksystem.Task
import dagr.pipelines.ParallelHaplotypeCaller.VariantCallTask
import dagr.sopt._
import dagr.tasks.DagrDef._
import dagr.tasks.gatk.{GenotypeGvcfs, HaplotypeCaller, SplitIntervalsForCallingRegions}
import dagr.tasks.misc.DeleteFiles
import dagr.tasks.parallel.PathBasedScatterGatherPipeline
import dagr.tasks.parallel.PathBasedScatterGatherPipeline.{GatherPathTask, ScatterPathTask}
import dagr.tasks.picard.{FilterVcf, GatherVcfs}

object ParallelHaplotypeCaller {

  /** Runs GenotypeGvcfs, HaplotypeCaller, and optionally FilterVcf on a given interval list. */
  private class VariantCallTask(ref: PathToFasta,
                                bam: PathToBam,
                                filterVcf: Boolean = true,
                                dbsnp: Option[PathToVcf] = None,
                                tmpDirectory: Option[Path] = None,
                                intervals: PathToIntervals,
                                useNativePairHmm: Boolean) extends ScatterPathTask {
    var _output: Option[Path] = None
    def output: Path = _output.get

    def getTasks: Traversable[_ <: Task] = {
      val tmpDir: Path = getTempDirectory(tmpDirectory, "variant_calling")
      val gvcf: PathToVcf     = Files.createTempFile(tmpDir, "gvcf.", ".vcf.gz")
      val finalVcf: PathToVcf = Files.createTempFile(tmpDir, "final_vcf.", ".vcf.gz")
      val hc    = new HaplotypeCaller(ref=ref, intervals=Some(intervals), bam=bam, vcf=gvcf, useNativePairHmm=useNativePairHmm)
      val gtGen = (vcf: PathToVcf) => GenotypeGvcfs(ref=ref, intervals=Some(intervals), gvcf=gvcf, vcf=vcf, dbSnpVcf=dbsnp)
      _output = Some(finalVcf)
      if (filterVcf) {
        val unfilteredVcf: PathToVcf = Files.createTempFile(tmpDir, "unfilterd_vcf.", ".vcf.gz")
        val gt   = gtGen(unfilteredVcf)
        val fvcf = new FilterVcf(in=unfilteredVcf, out=finalVcf)
        val del  = new DeleteFiles(Seq(gvcf, unfilteredVcf):_*)
        hc ==> gt ==> fvcf ==> del
        List(hc, gt, fvcf, del)
      }
      else {
        val gt  = gtGen(finalVcf)
        val del = new DeleteFiles(gvcf)
        hc ==> gt ==> del
        List(hc, gt, del)
      }
    }
  }

  private def getTempDirectory(tmpDirectory: Option[Path], suffix: String): Path = tmpDirectory match {
    case Some(t) => Files.createTempDirectory(t, suffix)
    case None => Files.createTempDirectory(suffix)
  }
}

// TODO: could also use IntervalListTools to break it up n-ways
// TODO: could also take in an interval list rather than use the whole genome

@clp(
description =
  """
  |Calls germline variants in a single sample and then optionally hard filters them.
  |
  |Runs GenotypeGvcfs, HaplotypeCaller, and optionally FilterVcf in parallel by splitting the interval list into
  |multiple non-overlapping regions.  If no interval list is given, runs on the entire reference.
  """,
  group = classOf[Pipelines]
)
class ParallelHaplotypeCaller
( @arg(flag="r", doc="Path to the reference FASTA.")                                val ref: PathToFasta,
  @arg(flag="i", doc="The input BAM file (indexed) from which to call variants.")   val bam: PathToBam,
  @arg(flag="l", doc="Path to interval list of regions to call.")                   val intervals: Option[PathToIntervals] = None,
  @arg(flag="o", doc="The output VCF to which to write variants.")                  val vcf: PathToVcf,
  @arg(flag="d", doc="Path to dbSNP VCF (for GenotypeGvcfs).")                      val dbsnp: Option[PathToVcf] = None,
  @arg(flag="f", doc="Filter variants using Picard FilterVcf.")                     val filterVcf: Boolean = false,
  @arg(flag="s", doc="The size of the region to call for each parallel task.")      val regionSize: Int = 25000000,
  @arg(flag="t", doc="Temporary directory in which to store intermediate results.") val tmpDirectory: Option[Path],
  @arg(flag="H", doc="Use the native code for GATK (Unix only)")                    val useNativePairHmm: Boolean = false
)
extends PathBasedScatterGatherPipeline(
  input                       = ref,
  output                      = vcf,
  splitInputPathTaskGenerator = (tmpDir: Option[Path]) => (ref: Path)             =>
    new SplitIntervalsForCallingRegions(ref=ref, intervals=intervals, output=Some(ParallelHaplotypeCaller.getTempDirectory(tmpDir, "intervals_for_calling")), regionSize=regionSize),
  scatterTaskGenerator        = (tmpDir: Option[Path]) => (intv: PathToIntervals) =>
    new VariantCallTask(ref=ref, bam=bam, filterVcf=filterVcf, dbsnp=dbsnp, tmpDirectory=tmpDir, intervals=intv, useNativePairHmm=useNativePairHmm),
  gatherTaskGenerator         = (in: Seq[PathToVcf], out: PathToVcf)              =>
    new GatherVcfs(in=in, out=out) with GatherPathTask,
  tmpDirectory                = tmpDirectory
)
