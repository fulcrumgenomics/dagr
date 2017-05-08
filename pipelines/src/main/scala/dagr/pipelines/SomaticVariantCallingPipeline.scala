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

import dagr.core.cmdline.Pipelines
import com.fulcrumgenomics.sopt._
import dagr.core.tasksystem.{NoOpInJvmTask, Pipeline, ShellCommand}
import dagr.tasks.DagrDef
import DagrDef._
import dagr.tasks.gatk.{Mutect1, Mutect2}
import dagr.tasks.misc.{DeleteFiles, DeleteVcfs}
import dagr.tasks.picard.IntervalListToBed
import dagr.tasks.samtools.SamtoolsPileup
import dagr.tasks.vc.{FilterFreeBayesCalls, FreeBayesSomatic, Varscan2Somatic}

/**
  * Pipeline to call Somatic Variants using both Varscan2 and Mutect2
  */
@clp(
  description =
    """
      |Calls somatic variants in a single sample [pair] and then filters them.
    """",
  group = classOf[Pipelines]
)
class SomaticVariantCallingPipeline
( @arg(flag='t', doc="The tumor BAM file.")                                       val tumorBam: PathToBam,
  @arg(flag='n', doc="The matched normal BAM file.")                              val normalBam: PathToBam,
  @arg(flag='r', doc="Path to the reference FASTA.")                              val ref: PathToFasta,
  @arg(flag='l', doc="Intervals to call over.")                                   val intervals: PathToIntervals,
  @arg(flag='o', doc="Output prefix (including directories) for output files.")   val outputPrefix: DirPath,
  @arg(          doc="Run MuTect2. Off by default since it is so slow.")          val includeMutect2: Boolean = false,
  @arg(          doc="Run FreeBayes.")                                            val includeFreeBayes: Boolean = false,
  @arg(          doc="If true, remove all intermediate files created.")           val removeIntermediates: Boolean = false
) extends Pipeline(Some(outputPrefix.toAbsolutePath.getParent)) {

  override def build(): Unit = {
    val outputDir = outputPrefix.toAbsolutePath.getParent
    val prefix = outputPrefix.getFileName.toString

    val targetBed        = outputDir.resolve(prefix + ".targets.bed")
    val tumorPileupFile  = outputDir.resolve(prefix + ".tumor.pileup")
    val normalPileupFile = outputDir.resolve(prefix + ".normal.pileup")
    val varscanDir       = outputDir.resolve(prefix + ".varscan2")
    val mutect2Vcf       = outputDir.resolve(prefix + ".mutect2.vcf")
    val mutect1Vcf       = outputDir.resolve(prefix + ".mutect1.vcf")
    val mutect1Callstats = outputDir.resolve(prefix + ".mutect1.callstats.txt")
    val freeBayesVcf     = outputDir.resolve(prefix + ".freeebayes.vcf")

    val mkdir     = ShellCommand("mkdir", "-p", outputDir.toAbsolutePath.toString)
    val makeBed   = new IntervalListToBed(intervals=intervals, bed=targetBed)
    val tpileup   = new SamtoolsPileup(ref=ref, regions=Some(targetBed), bam=tumorBam, out=Some(tumorPileupFile))
    val npileup   = new SamtoolsPileup(ref=ref, regions=Some(targetBed), bam=normalBam, out=Some(normalPileupFile))
    val varscan   = new Varscan2Somatic(tumorPileupFile, normalPileupFile, varscanDir)
    val mutect1   =
      new Mutect1(tumorBam=tumorBam, normalBam=Some(normalBam), ref=ref, intervals=intervals, vcfOutput=mutect1Vcf, callStatsOutput=mutect1Callstats)
    val mutect2   = if (includeMutect2)
        new Mutect2(tumorBam=tumorBam, normalBam=normalBam, ref=ref, intervals=intervals, output=mutect2Vcf)
      else
        new NoOpInJvmTask("MuTect2NoOp")

    val freeBayes =
      if (includeFreeBayes) {
        val unfilteredVcf = outputDir.resolve(prefix + ".freeebayes.unfiltered.vcf")
        val fb = new FreeBayesSomatic(ref=ref, Some(intervals), tumorBam=tumorBam, normalBam=normalBam, vcf=unfilteredVcf, compress=false)
        val filterVcf = new FilterFreeBayesCalls(in=unfilteredVcf, out=freeBayesVcf, ref=ref, compress=false)
        fb ==> filterVcf ==> new DeleteVcfs(unfilteredVcf)
        fb
      }
      else {
        new NoOpInJvmTask("FreeBayesNoOp")
      }

    root ==> mkdir ==> makeBed ==> (tpileup :: npileup) ==> varscan
             mkdir ==> (mutect1 :: mutect2 :: freeBayes)

    if (removeIntermediates) {
      (varscan :: mutect1 :: mutect2) ==> new DeleteFiles(targetBed, tumorPileupFile, normalPileupFile)
    }
  }
}
