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

import java.nio.file.Path

import com.fulcrumgenomics.commons.io.PathUtil
import dagr.core.cmdline.Pipelines
import com.fulcrumgenomics.sopt._
import dagr.core.tasksystem.{Linker, Pipeline, ShellCommand}
import com.fulcrumgenomics.sopt.cmdline.ValidationException
import dagr.tasks.DagrDef
import DagrDef._
import dagr.tasks.gatk.{GenotypeGvcfs, HaplotypeCaller}
import dagr.tasks.misc.{DeleteVcfs, GetSampleNamesFromVcf, IndexVcfGz}
import dagr.tasks.picard.{CollectVariantCallingMetrics, FilterVcf, GenotypeConcordance, UpdateVcfSequenceDictionary}
import dagr.tasks.vc.{FilterFreeBayesCalls, FreeBayesGermline}

/**
  * Pipeline to call germline variants in a set of regions
  */
@clp(
description =
  """
    |Calls germline variants in a single sample and then hard filters them.
    |For GATK, calls are made using the GATK's HaplotypeCaller and filtering with
    |Picard's FilterVcf.  For FreeBayes, calls are made using bcbio's best practices.
    |Optionally, variants are assessed with Picard's GenotypeConcordance tool.
    |""",
  group = classOf[Pipelines]
)
class GermlineVariantCallingPipeline
( @arg(flag='i', doc="The input BAM file (indexed) from which to call variants.")   val in: PathToBam,
  @arg(flag='o', doc="Output prefix (including directories) for output files.")     val out: Path,
  @arg(flag='l', doc="Path to interval list of regions to call.")                   val intervals: Option[PathToIntervals] = None,
  @arg(flag='r', doc="Path to the reference FASTA.")                                val ref: PathToFasta,
  @arg(flag='d', doc="Path to dbSNP VCF (for GATK and CollectVariantCallingMetrics only).")
            val dbsnp: Option[PathToVcf] = None,
  @arg(          doc="If true, keep intermediate VCFs (GATK only).")                val keepIntermediates: Boolean = false,
  @arg(          doc="Use GATK (true), otherwise use FreeBayes (false).")           val useGatk: Boolean = true,
  @arg(          doc="The VCF with truth calls for assessing variants.")            val truthVcf: Option[PathToVcf] = None,
  @arg(          doc="Path to the interval list(s) of regions to assess variants.", minElements = 0)
            val truthIntervals: List[PathToIntervals] = Nil
) extends Pipeline(Some(out.toAbsolutePath.getParent)) {

  private val outputDir = out.toAbsolutePath.getParent
  private val prefix = out.getFileName.toString
  private val unfilteredVcf = outputDir.resolve(prefix + ".unfiltered.vcf.gz")
  private val finalVcf = outputDir.resolve(prefix + ".vcf.gz")

  override def build(): Unit = {

    // Run the variant caller
    val vc = {
      val filterVcf = useGatk match {
        case true =>
          val gvcf = outputDir.resolve(prefix + ".gvcf.vcf.gz")
          val hc   = new HaplotypeCaller(ref=ref, intervals=intervals, bam=in, vcf=gvcf)
          val gt   = GenotypeGvcfs(ref=ref, intervals=intervals, gvcf=gvcf, vcf=unfilteredVcf, dbSnpVcf=dbsnp)
          val fvcf = new FilterVcf(in=unfilteredVcf, out=finalVcf)
          root ==> hc ==> gt ==> (fvcf :: new DeleteVcfs(gvcf))
        case false =>
          val dict = PathUtil.replaceExtension(ref, ".dict")
          val tVcf = outputDir.resolve(prefix + ".unfiltered.noheader.vcf.gz")
          val fb   = new FreeBayesGermline(ref=ref, targetIntervals=intervals, bam=List(in), vcf=tVcf)
          val ugh  = new UpdateVcfSequenceDictionary(in=tVcf, out=unfilteredVcf, dict=dict) // Ugh!
          val fvcf = new FilterFreeBayesCalls(in=unfilteredVcf, out=finalVcf, ref=ref)
          val del  = new DeleteVcfs(tVcf)
          root ==> fb ==> ugh ==> (del :: fvcf)
      }
      if (!keepIntermediates) filterVcf ==> new DeleteVcfs(unfilteredVcf)
      filterVcf ==> new IndexVcfGz(vcf=finalVcf)
    }

    // Run variant calling metrics
    dbsnp match {
      case Some(db) => vc ==> new CollectVariantCallingMetrics(vcf=finalVcf, prefix=out, dbsnp=db, intervals=intervals)
      case _ => Unit
    }

    // Perform variant assessment
    truthVcf match {
      case Some(tVcf) =>
        // Get the sample names from each VCF.  Assume each VCF has only one sample. */
        val getCallSampleName   = new GetSampleNamesFromVcf(finalVcf)
        val getTruthSampleName  = new GetSampleNamesFromVcf(tVcf)
        // Run genotype concordance, but only after we have the truth and call sample names.
        val concordanceIntervals = truthIntervals ++ intervals.map(List(_)).getOrElse(Nil)
        val concordance = new GenotypeConcordance(
          truthVcf    = tVcf,
          callVcf     = finalVcf,
          callSample  = "NA",
          truthSample = "NA",
          prefix      = out,
          intervals   = concordanceIntervals
        )
        def checkAndSetSampleName(isCallSample: Boolean)(sn: GetSampleNamesFromVcf, gc: GenotypeConcordance): Unit = {
          sn.sampleNames match {
            case Nil => throw new IllegalStateException(s"Found no sample names in '${sn.vcf}")
            case sampleName :: Nil =>
              if (isCallSample) gc.callSample = sampleName
              else gc.truthSample = sampleName
            case x :: xs =>
              throw new IllegalStateException(s"Found more than one sample names in '${sn.vcf}: " + sn.sampleNames.mkString(", "))
          }
        }

        vc ==> getCallSampleName  ==> Linker(getCallSampleName,  concordance)(checkAndSetSampleName(isCallSample=true))  ==> concordance
        vc ==> getTruthSampleName ==> Linker(getTruthSampleName, concordance)(checkAndSetSampleName(isCallSample=false)) ==> concordance

        // Update the dependencies
        vc ==> (getCallSampleName :: getTruthSampleName) ==> concordance
      case None => Unit
    }
  }
}
