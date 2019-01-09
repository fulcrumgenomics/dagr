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

package dagr.tasks.vc

import java.nio.file.Path

import com.fulcrumgenomics.commons.CommonsDef.yieldAndThen
import com.fulcrumgenomics.commons.io.{Io, PathUtil}
import dagr.core.cmdline.Pipelines
import dagr.core.config.Configuration
import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.Pipes.PipeWithNoResources
import dagr.core.tasksystem._
import com.fulcrumgenomics.sopt.{arg, clp}
import dagr.tasks.DagrDef._
import dagr.tasks.misc.DeleteFiles
import dagr.tasks.picard.SortVcf
import htsjdk.samtools.SamReaderFactory

import scala.collection.mutable.ListBuffer

/**
  * The VarDictJava directory structure is assumed to be as follows:
  *   - Root directory:    /path/to/git/VarDictJava
  *   - VarDict Submodule: /path/to/git/VarDictJava/VarDict
  *   - Startup script:    /path/to/git/VarDictJava/build/install/VarDict/bin/VarDict
  */
object VarDictJava extends Configuration {
  /** Config key for the VarDictJava directory.  This should be the root project directory. */
  val RootDirConfigKey = "vardictjava.dir"

  /** Path to the VarDictJava directory. */
  val RootDir = configure[Path](RootDirConfigKey)

  /** Path to the VarDictJava install directory with the VarDictJava startup script. */
  val VarDictJava = RootDir.resolve("build/install/VarDict/bin/VarDict")

  /** Path to the VarDict submodule with a multitude of perl and R scripts. */
  val BinDir = RootDir.resolve("VarDict")

  /** Path to the teststrandbias.R script in the VarDict submodule. */
  val TestStrandBias = BinDir.resolve("teststrandbias.R")

  /** Path to the var2vcf_valid.pl script in the VarDict submodule. */
  val Var2VcfValid = BinDir.resolve("var2vcf_valid.pl")

  /** Pulls a sample name out of a BAM file. */
  private[vc] def extractSampleName(bam: PathToBam): String = {
    val in = SamReaderFactory.make().open(bam)
    yieldAndThen(in.getFileHeader.getReadGroups.iterator().next().getSample) { in.close() }
  }
}

/** Runs VarDictJava to produce its intermediate tabular format. */
private class VarDictJava(tumorBam: PathToBam,
                          bed: FilePath,
                          ref: PathToFasta,
                          tumorName: String,
                          minimumAf: Double = 0.01,
                          includeNonPf: Boolean = false,
                          minimumQuality: Option[Int] = None,
                          maximumMismatches: Option[Int] = None,
                          minimumAltReads: Option[Int] = None,
                          pileupMode: Boolean = false,
                          countNsInTotalDepth: Boolean = false,
                          minThreads: Int = 1,
                          maxThreads: Int = 32,
                          memory: Memory = Memory("8G")
                         ) extends ProcessTask with VariableResources with PipeOut[Any] {
  name = "VarDictJava"

  override def pickResources(resources: ResourceSet): Option[ResourceSet] = {
    resources.subset(minCores = Cores(minThreads), maxCores = Cores(maxThreads), memory = memory)
  }

  override def args: Seq[Any] = {
    val buffer = new ListBuffer[Any]()
    buffer.appendAll(List(VarDictJava.VarDictJava, "-G", ref, "-N", tumorName, "-b", tumorBam))
    if (pileupMode) buffer.append("-p")
    buffer.append("-z", "1") // Set to 1 since we are using a BED as input
    buffer.append("-c", "1") // The column for the chromosome
    buffer.append("-S", "2") // The column for the region start
    buffer.append("-E", "3") // The column for the region end
    buffer.append("-g", "4") // The column for the gene name
    if (!includeNonPf) buffer.append("-F", "0x700") // ignore non-PF reads
    buffer.append("-f", minimumAf) // The minimum allele frequency threshold
    minimumAltReads.foreach(buffer.append("-r", _)) // Minimum # of reads supporting an alternate allele
    minimumQuality.foreach(buffer.append("-q", _)) // The minimum base quality for a "good call"
    maximumMismatches.foreach(buffer.append("-m", _)) // Maximum number of mismatches for reads to be included, otherwise they will be filtered.
    if (countNsInTotalDepth) buffer.append("-K") // count Ns in the total depth ("DP" field)
    buffer.append("-th", resources.cores.toInt) // The number of threads.
    buffer.append(bed)

    buffer
  }
}

/** Converts VarDictJava tabular output to valid VCF output. */
private class Var2VcfValid(sampleName: String,
                           includeNonPfVariants: Boolean = false,
                           allVariants: Boolean = true,
                           maximumDistanceToRemoveHighQualitySnvPairs: Option[Int] = None,
                           maximumNonMonomerMsi: Option[Int] = None,
                           maximumMeanMismatches: Option[Double] = None,
                           minimumMeanVariantPositionInReads: Option[Double] = None,
                           minimumMeanBaseQuality: Option[Double] = None,
                           minimumMappingQuality: Option[Double] = None,
                           minimumDepth: Option[Int] = None,
                           minimumHighQualityAltDepth: Option[Int] = None,
                           minimumAf: Double = 0.01,
                           minimumSignalToNoiseRatio: Option[Double] = None,
                           minimumHomozygousAlleleFreq: Option[Double] = None,
                           printEndTag: Boolean = false,
                           minimumSplitReadsForSv: Option[Int] = None
                          ) extends ProcessTask with PipeWithNoResources[Any, Any] {
  name = "Var2VcfValid"

  override def args: Seq[Any] = {
    val buffer = new ListBuffer[Any]()
    buffer.appendAll(List(VarDictJava.Var2VcfValid, "-N", sampleName))
    if (!includeNonPfVariants) buffer.append("-S") // If set, variants that did not pass filters will not be present.
    if (allVariants) buffer.append("-A") // Output all variants at the same position, default outputs only the highest AF variant.
    maximumDistanceToRemoveHighQualitySnvPairs.foreach(buffer.append("-c", _)) // If two seemingly high quality SNV variants are within {int}bp, they're both filtered.
    maximumNonMonomerMsi.foreach(buffer.append("-I", _)) // The maximum non-monomer MSI allowed for a HT variant with AF < 0.5.
    maximumMeanMismatches.foreach(buffer.append("-m", _)) // Maximum mean mismatches per read allowed, does not include indels.
    minimumMeanVariantPositionInReads.foreach(buffer.append("-p", _)) // The minimum mean position of variants in the read.
    minimumMeanBaseQuality.foreach(buffer.append("-q", _)) // The minimum mean base quality.
    minimumMappingQuality.foreach(buffer.append("-Q", _)) // The minimum mapping quality.
    minimumDepth.foreach(buffer.append("-d", _)) // The minimum total read depth.
    minimumHighQualityAltDepth.foreach(buffer.append("-v", _)) // The minimum high quality variant depth.
    buffer.append("-f", minimumAf) // The minimum allele frequency.
    minimumSignalToNoiseRatio.foreach(buffer.append("-o", _)) // The minimum signal to noise, or the ratio of hi/(lo+0.5).
    minimumHomozygousAlleleFreq.foreach(buffer.append("-F", _)) // The minimum allele frequency to consider a variant as homozygous.
    if (!printEndTag) buffer.append("-E") // If set, do not print END tag.
    minimumSplitReadsForSv.foreach(buffer.append("-T", _)) // The minimum number of split reads for an SV call.

    buffer
  }
}

/** Pipeline to call variants from a tumor-only sample using VarDictJava. */
@clp(
  description =
    """
      |Calls somatic variants in a tumor-only sample using VarDictJava.
      |
      |Tumor-only single-sample variant calling with VarDictJava: https://github.com/AstraZeneca-NGS/VarDictJava
      |
      |The output Tumor name will be inferred from the the first read group in the input BAM if not provided.
    """",
  group = classOf[Pipelines]
)
class VarDictJavaEndToEnd
(@arg(flag='i', doc="The input tumor BAM file.") tumorBam: PathToBam,
 @arg(flag='l', doc="The intervals over which to call variants.") bed: FilePath,
 @arg(flag='r', doc="Path to the reference fasta file.") ref: PathToFasta,
 @arg(flag='o', doc="The output VCF.") out: PathToVcf,
 @arg(flag='n', doc="The tumor sample name, otherwise taken from the first read group in the BAM.") tumorName: Option[String] = None,
 @arg(flag='f', doc="The minimum allele frequency.") minimumAf: Double = 0.01,
 @arg(flag='p', doc="Include non-pf reads.") includeNonPf: Boolean = false,
 @arg(flag='S', doc="Include non-pf calls.") includeNonPfVariants: Boolean = true,
 @arg(flag='m', doc="The minimum base quality for a read to support a call.") minimumQuality: Option[Int] = None,
 @arg(flag='M', doc="The maximum # of mismatches a read may have to support a call (not indels).") maximumMismatches: Option[Int] = None,
 @arg(flag='s', doc="The maximum mean # of mismatches per read for a call (not indels).") maximumMeanMismatches: Option[Double] = None,
 @arg(flag='R', doc="The minimum # of reads with alternate bases.") minimumAltReads: Option[Int] = None,
 @arg(flag='x', doc="The minimum mean position in reads for a call.") minimumMeanVariantPositionInReads: Option[Double] = None,
 @arg(flag='q', doc="The minimum mean base quality for a call.") minimumMeanBaseQuality: Option[Double] = None,
 @arg(flag='d', doc="The minimum total read depth for a call.") minimumDepth: Option[Int] = None,
 @arg(flag='v', doc="The minimum # of high quality alternate alleles for a call.") minimumHighQualityAltDepth: Option[Int] = None,
 @arg(flag='P', doc="Use the pileup mode (emit all sites with an alternate).") pileupMode: Boolean = false,
 @arg(flag='t', doc="The minimum # of threads with which to run.") minThreads: Int = 1,
 @arg(flag='T', doc="The maximum # of threads with which to run.") maxThreads: Int = 32,
 @arg(flag='a', doc="Output all sites, including reference calls.") allSites: Boolean = false,
 @arg(flag='A', doc="Output all variants at the same genomic site.") allVariants: Boolean = false,
 @arg(flag='N', doc="Count No-calls (Ns) in the total depth tag (DP)") countNsInTotalDepth: Boolean = false) extends Pipeline {

  if (allSites) require(pileupMode, "pileup-mode is required when all-sites is used")

  def build(): Unit = {
    val tn = tumorName.getOrElse(VarDictJava.extractSampleName(bam = tumorBam))
    val dict = PathUtil.replaceExtension(ref, ".dict")

    def f(ext: String): Path = Io.makeTempFile("vardict.", ext, dir = if (out == Io.StdOut) None else Some(out.getParent))

    val tmpVcf = f(".vcf")

    val vardict = new VarDictJava(
      tumorBam            = tumorBam,
      bed                 = bed,
      ref                 = ref,
      tumorName           = tn,
      minimumAf           = minimumAf,
      includeNonPf        = includeNonPf,
      minimumQuality      = minimumQuality,
      maximumMismatches   = maximumMismatches,
      minimumAltReads     = minimumAltReads,
      pileupMode          = pileupMode,
      countNsInTotalDepth = countNsInTotalDepth,
      minThreads          = minThreads,
      maxThreads          = maxThreads
    )

    val removeRefEqAltRows = if (allSites) Pipes.empty[Any] else new ShellCommand("awk", "{if ($6 != $7) print}") with PipeWithNoResources[Any, Any]
    val bias = new ShellCommand(VarDictJava.TestStrandBias.toString) with PipeWithNoResources[Any, Any]

    val var2vcfvalid = new Var2VcfValid(
      sampleName                        = tn,
      includeNonPfVariants              = includeNonPfVariants,
      allVariants                       = allVariants,
      maximumMeanMismatches             = maximumMeanMismatches,
      minimumMeanVariantPositionInReads = minimumMeanVariantPositionInReads,
      minimumMeanBaseQuality            = minimumMeanBaseQuality,
      minimumDepth                      = minimumDepth,
      minimumHighQualityAltDepth        = minimumHighQualityAltDepth,
      minimumAf                         = minimumAf,
      printEndTag                       = false
    )

    val sortVcf = new SortVcf(in = tmpVcf, out = out, dict = Some(dict))

    root ==> (vardict | removeRefEqAltRows | bias | var2vcfvalid > tmpVcf).withName("VarDictJavaPipeChain") ==> sortVcf ==> new DeleteFiles(tmpVcf)
  }
}
