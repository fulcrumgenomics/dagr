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

import dagr.core.cmdline._
import dagr.core.tasksystem.{Callbacks, Pipeline, ShellCommand}
import dagr.tasks._
import dagr.tasks.gatk.{GenotypeSingleGvcf, HaplotypeCaller}
import dagr.tasks.misc.{DeleteVcfs, GetSampleNamesFromVcf, IndexVcfGz}
import dagr.tasks.picard.{CollectVariantCallingMetrics, FilterVcf, GenotypeConcordance}
import dagr.tasks.vc.{FilterFreeBayesCalls, FreeBayesGermline}

/**
  * Pipeline to call germline variants in a set of regions
  *
  */
@CLP(
description =
  """
    |Calls germline variants in a single sample and then hard filters them.
    |For GATK, calls are made using the GATK's HaplotypeCaller and filtering with
    |Picard's FilterVcf.  For FreeBayes, calls are made using bcbio's best practices.
    |Optionally, variants are assessed with Picard's GenotypeConcordance tool.
    |"""
)
class GermlineVariantCallingPipeline(
  @Arg(flag="i", doc="The input BAM file (indexed) from which to call variants.")   val input: PathToBam,
  @Arg(flag="o", doc="Output prefix (including directories) for output files.")     val outputPrefix: Path,
  @Arg(flag="l", doc="Path to interval list of regions to call.")                   val intervals: PathToIntervals,
  @Arg(flag="r", doc="Path to the reference FASTA.")                                val reference: PathToFasta,
  @Arg(flag="d", doc="Path to dbSNP VCF (for GATK and CollectVariantCallingMetrics only).")
                                                                                    val dbsnp: Option[PathToVcf] = None,
  @Arg(          doc="If true, keep intermediate VCFs (GATK only).")                val keepIntermediates: Boolean = false,
  @Arg(          doc="Use GATK (true), otherwise use FreeBayes (false).")           val useGatk: Boolean = true,
  @Arg(          doc="The VCF with truth calls for assessing variants.")            val truthVcf: Option[PathToVcf] = None,
  @Arg(          doc="Path to the interval list(s) of regions to assess variants.", minElements = 0)
                                                                                    val truthIntervals: List[PathToIntervals] = Nil
) extends Pipeline(Some(outputPrefix.toAbsolutePath.getParent)) {

  override def build(): Unit = {
    val outputDir = outputPrefix.toAbsolutePath.getParent
    val prefix = outputPrefix.getFileName.toString
    val unfilteredVcf = outputDir.resolve(prefix + ".unfiltered.vcf.gz")
    val finalVcf = outputDir.resolve(prefix + ".vcf.gz")

    // Ensure the output directory exists
    val mkdir = ShellCommand("mkdir", "-p", outputDir.toAbsolutePath)
    root ==> mkdir

    // Run the variant caller
    val vc = {
      val filterVcf = useGatk match {
        case true =>
          val gvcf = outputDir.resolve(prefix + ".gvcf.vcf.gz")
          val hc = new HaplotypeCaller(reference=reference, targetIntervals=intervals, bam=input, vcf=gvcf)
          val gt = new GenotypeSingleGvcf(reference=reference, targetIntervals=intervals, gvcf=gvcf, vcf=unfilteredVcf, dbSnpVcf=dbsnp)
          val fvcf = new FilterVcf(input=unfilteredVcf, output=finalVcf)
          mkdir ==> hc ==> gt ==> (fvcf :: new DeleteVcfs(gvcf))
        case false =>
          val fb =  new FreeBayesGermline(reference=reference, targetIntervals=Some(intervals), bam=List(input), vcf=unfilteredVcf)
          val fvcf = new FilterFreeBayesCalls(input=unfilteredVcf, output=finalVcf, reference=reference)
          mkdir ==> fb ==> fvcf
      }
      if (!keepIntermediates) filterVcf ==> new DeleteVcfs(unfilteredVcf)
      filterVcf ==> new IndexVcfGz(vcf=finalVcf)
    }

    // Run variant calling metrics
    if (dbsnp.isDefined) {
      val vcMetrics = new CollectVariantCallingMetrics(vcf=finalVcf, prefix=outputPrefix, dbsnp=dbsnp.get, intervals=Some(intervals))
      vc ==> vcMetrics
    }

    // Perform variant assessment
    if (truthVcf.isDefined) {
      // Get the sample names from each VCF.  Assume each VCF has only one sample. */
      val getCallSampleName = new GetSampleNamesFromVcf(finalVcf)
      val getTruthSampleName = new GetSampleNamesFromVcf(truthVcf.get)
      // Run genotype concordance, but only after we have the truth and call sample names.
      val genotypeConcordance = new GenotypeConcordance(
        truthVcf    = truthVcf.get,
        callVcf     = finalVcf,
        callSample  = "NA",
        truthSample = "NA",
        prefix      = outputPrefix,
        intervals   = truthIntervals ++ List(intervals)
      )
      def checkAndSetSampleName(sn: GetSampleNamesFromVcf, gc: GenotypeConcordance, isCallSample: Boolean): Unit = {
        if (sn.sampleNames.isEmpty) throw new IllegalStateException(s"Found no sample names in '${sn.vcf}")
        else if (sn.sampleNames.length > 1) throw new IllegalStateException(s"Found more than one sample names in '${sn.vcf}: " + sn.sampleNames.mkString(", "))
        if (isCallSample) gc.callSample = sn.sampleNames.head
        else gc.truthSample = sn.sampleNames.head
      }
      // Connect genotype concordance to getting the sample name
      Stream((getCallSampleName, true), (getTruthSampleName, false)).foreach{
        case (getSampleName, isCallSample) =>
          Callbacks.connect(genotypeConcordance, getSampleName)((gc, sn) => checkAndSetSampleName(sn, gc, isCallSample))
      }
      // Update the dependencies
      vc ==> (getCallSampleName :: getTruthSampleName) ==> genotypeConcordance
    }
  }
}