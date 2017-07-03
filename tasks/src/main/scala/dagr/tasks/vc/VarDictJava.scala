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
import dagr.core.tasksystem.Pipes.PipeWithNoResources
import dagr.core.tasksystem._
import com.fulcrumgenomics.sopt.{arg, clp}
import dagr.api.models.{Cores, Memory, ResourceSet}
import dagr.tasks.DagrDef._
import dagr.tasks.DataTypes.Vcf
import dagr.tasks.misc.DeleteFiles
import dagr.tasks.picard.{IntervalListToBed, SortVcf}
import htsjdk.samtools.SamReaderFactory

import scala.collection.mutable.ListBuffer

/**
  * The VarDictJava directory structure is assumed to be as follows:
  *   - Root directory: /path/to/git/VarDictJava
  *   - Perl scripts:   /path/to/git/VarDictJava/VarDict
  *   - Startup script: /path/to/git/VarDictJava/build/install/VarDict/bin/VarDict
  */
object VarDictJava extends Configuration {
  /** Config key for the VarDictJava directory.  This should be the root project directory. */
  val RootDirConfigKey = "vardictjava.dir"

  /** Path to the VarDictJava directory. */
  val RootDir = configure[Path](RootDirConfigKey)

  /** Path to the VarDictJava install directory with the VarDictJava startup script. */
  val VarDictJava = RootDir.resolve("build/install/VarDict/bin/VarDict")

  /** Path to the VarDictJava bin directory with a multitude of perl scripts. */
  val BinDir = RootDir.resolve("VarDict")

  /** Pulls a sample name out of a BAM file. */
  private[vc] def extractSampleName(bam: PathToBam) : String = {
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
                          minThreads: Int = 1,
                          maxThreads: Int = 32,
                          memory: Memory = Memory("8G")
                         ) extends ProcessTask with VariableResources with PipeOut[Any] {
  name = "VarDictJava"

  override def pickResources(resources: ResourceSet): Option[ResourceSet] = {
    resources.subset(minCores = Cores(minThreads), maxCores = Cores(maxThreads), memory = memory)
  }

  override def args: Seq[Any] = {
    val buffer   = new ListBuffer[Any]()
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
    buffer.append("-th", resources.cores.toInt) // The number of threads.
    buffer.append(bed)

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
 @arg(flag='m', doc="The minimum base quality to use.") minimumQuality: Option[Int] = None,
 @arg(flag='M', doc="he maximum # of mismatches (not indels).") maximumMismatches: Option[Int] = None,
 @arg(flag='R', doc="The minimum # of reads with alternate bases.") minimumAltReads: Option[Int] = None,
 @arg(flag='P', doc="Use the pileup mode (emit all sites with an alternate).") pileupMode: Boolean = false,
 @arg(flag='t', doc="The minimum # of threads with which to run.") minThreads: Int = 1,
 @arg(flag='T', doc="The maximum # of threads with which to run.") maxThreads: Int = 32) extends Pipeline {
  import VarDictJava.BinDir

  // Put these here so that they'll error at construction if missing
  private val biasScript = BinDir.resolve("teststrandbias.R")
  private val vcfScript  = BinDir.resolve("var2vcf_valid.pl")

  def build(): Unit = {
    val tn = tumorName.getOrElse(VarDictJava.extractSampleName(bam=tumorBam))
    val dict = PathUtil.replaceExtension(ref, ".dict")
    def f(ext: String): Path = Io.makeTempFile("vardict.", ext, dir= if (out == Io.StdOut) None else Some(out.getParent))
    val tmpVcf = f(".vcf")

    val vardict = new VarDictJava(
      tumorBam          = tumorBam,
      bed               = bed,
      ref               = ref,
      tumorName         = tn,
      minimumAf         = minimumAf,
      includeNonPf      = includeNonPf,
      minimumQuality    = minimumQuality,
      maximumMismatches = maximumMismatches,
      minimumAltReads   = minimumAltReads,
      pileupMode        = pileupMode,
      minThreads        = minThreads,
      maxThreads        = maxThreads
    )
    val removeRefEqAltRows = new ShellCommand("awk", "{if ($6 != $7) print}") with PipeWithNoResources[Any,Any]
    val bias               = new ShellCommand(biasScript.toString) with PipeWithNoResources[Any,Any]
    val toVcf              = new ShellCommand(vcfScript.toString, "-N", tn, "-E", "-f", minimumAf.toString) with PipeWithNoResources[Any,Vcf]
    val sortVcf            = new SortVcf(in=tmpVcf, out=out, dict=Some(dict))

    root ==> (vardict | removeRefEqAltRows | bias | toVcf > tmpVcf).withName("VarDictJava") ==> sortVcf ==> new DeleteFiles(tmpVcf)
  }
}
