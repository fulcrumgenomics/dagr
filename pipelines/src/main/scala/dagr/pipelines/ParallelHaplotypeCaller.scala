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
 *
 */

package dagr.pipelines

import java.nio.file.{Files, Path}

import com.fulcrumgenomics.commons.io.PathUtil
import dagr.core.cmdline.Pipelines
import dagr.core.tasksystem.Pipeline
import com.fulcrumgenomics.sopt._
import dagr.tasks.DagrDef._
import dagr.tasks.ScatterGather.Scatter
import dagr.tasks.gatk.{GenotypeGvcfs, HaplotypeCaller, SplitIntervalsForCallingRegions}
import dagr.tasks.misc.DeleteFiles
import dagr.tasks.picard.{FilterVcf, GatherVcfs}

// TODO: could also use IntervalListTools to break it up n-ways

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
(@arg(flag='r', doc="Path to the reference FASTA.")                                val ref: PathToFasta,
 @arg(flag='i', doc="The input BAM file (indexed) from which to call variants.")   val input: PathToBam,
 @arg(flag='l', doc="Path to interval list of regions to call.")                   val intervals: Option[PathToIntervals] = None,
 @arg(flag='o', doc="The output VCF to which to write variants.")                  val output: PathToVcf,
 @arg(flag='d', doc="Path to dbSNP VCF (for GenotypeGvcfs).")                      val dbsnp: Option[PathToVcf] = None,
 @arg(flag='f', doc="Filter variants using Picard FilterVcf.")                     val filterVcf: Boolean = false,
 @arg(flag='s', doc="The size of the region to call for each parallel task.")      val maxBasesPerScatter: Int = 25000000,
 @arg(flag='t', doc="Temporary directory in which to store intermediate results.") val tmpDirectory: Option[Path],
 @arg(flag='H', doc="Use the native code for GATK (Unix only)")                    val useNativePairHmm: Boolean = false
)
extends Pipeline(outputDirectory = Some(output.getParent)) {
  private def replace(p: Path, a: String, b: String): Path = p.getParent.resolve(p.getFileName.toString.replace(a, b))

  override def build(): Unit = {
    val dir = tmpDirectory match {
      case Some(p) => Files.createTempDirectory(p, "parallel_hap")
      case None    => Files.createTempDirectory("parallel_hap")
    }

    val scatterer = new SplitIntervalsForCallingRegions(ref=ref, intervals=intervals, output=Some(dir), maxBasesPerScatter=maxBasesPerScatter)
    val scatter = Scatter(scatterer)

    val hc = scatter.map(il => new HaplotypeCaller(ref=ref, intervals=Some(il), bam=input, vcf=PathUtil.replaceExtension(il, ".g.vcf.gz"), useNativePairHmm=useNativePairHmm))
    val gt = hc.map(h => GenotypeGvcfs(ref=ref, intervals=h.intervals, gvcf=h.vcf, vcf=replace(h.vcf, ".g.vcf.gz", ".vcf.gz"), dbSnpVcf = dbsnp ))
    val gather = if (filterVcf) {
      val filter = gt.map(g => new FilterVcf(in=g.vcf, out=replace(g.vcf, ".vcf.gz", ".filtered.vcf.gz")))
      filter.gather(fs => new GatherVcfs(in=fs.map(_.out), out=output))
    }
    else {
      gt.gather(gts => new GatherVcfs(in=gts.map(_.vcf), out=output))
    }

    root ==> scatter
    gather ==> new DeleteFiles(dir)
  }
}
