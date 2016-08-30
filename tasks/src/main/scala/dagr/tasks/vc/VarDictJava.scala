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

import dagr.commons.CommonsDef.yieldAndThen
import dagr.commons.io.{Io, PathUtil}
import dagr.core.config.Configuration
import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem._
import dagr.tasks.DagrDef.{FilePath, PathToBam, PathToFasta, PathToVcf}
import dagr.tasks.DataTypes.Vcf
import dagr.tasks.picard.SortVcf
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
  lazy val RootDir = configure[Path](RootDirConfigKey)

  /** Path to the VarDictJava install directory with the VarDictJava startup script. */
  lazy val VarDictJava = RootDir.resolve("build/install/VarDict/bin/VarDict")

  /** Path to the VarDictJava bin directory with a multitude of perl scripts. */
  lazy val BinDir = RootDir.resolve("VarDict")

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
                          minThreads: Int = 1,
                          maxThreads: Int = 32,
                          memory: Memory = Memory("8G")
                         ) extends ProcessTask with VariableResources with PipeOut[Vcf] {
  name = "VarDictJava"

  override def pickResources(resources: ResourceSet): Option[ResourceSet] = {
    resources.subset(minCores = Cores(minThreads), maxCores = Cores(maxThreads), memory = memory)
  }

  override def args: Seq[Any] = {
    val vdScript = VarDictJava.VarDictJava
    val buffer   = new ListBuffer[Any]()

    buffer.appendAll(List(vdScript, "-G", ref, "-N", tumorName, "-b", tumorBam))
    buffer.append("-z", "1") // Set to 1 since we are using a BED as input
    buffer.append("-c", "1") // The column for the chromosome
    buffer.append("-S", "2") // The column for the region start
    buffer.append("-E", "3") // The column for the region end
    buffer.append("-g", "4") // The column for the gene name
    buffer.append("-f", minimumAf) // The minimum allele frequency threshold
    if (!includeNonPf) buffer.append("-F", "0x700") // ignore non-PF reads
    minimumQuality.foreach(buffer.append("-q", _)) // The minimum base quality for a "good call"
    maximumMismatches.foreach(buffer.append("-m", _)) // Maximum number of mismatches for reads to be included, otherwise they will be filtered.
    buffer.append("-th", resources.cores.toInt) // The number of threads.
    buffer.append(bed)

    buffer
  }
}

/**
  * Tumor-only single-sample variant calling with VarDictJava: https://github.com/AstraZeneca-NGS/VarDictJava
  *
  * The output Tumor name will be inferred from the the first read group in the input BAM if not provided.
  */
class VarDictJavaEndToEnd(tumorBam: PathToBam,
                          bed: FilePath,
                          ref: PathToFasta,
                          out: PathToVcf = Io.StdOut,
                          tumorName: Option[String] = None,
                          minimumAf: Double = 0.01,
                          includeNonPf: Boolean = false,
                          minimumQuality: Option[Int] = None,
                          maximumMismatches: Option[Int] = None,
                          minThreads: Int = 1,
                          maxThreads: Int = 32) extends Pipeline {
  import VarDictJava.BinDir

  def build(): Unit = {
    val tn = tumorName.getOrElse(VarDictJava.extractSampleName(bam=tumorBam))
    val dict = PathUtil.replaceExtension(ref, ".dict")

    val biasScript = BinDir.resolve("teststrandbias.R")
    val vcfScript  = BinDir.resolve("var2vcf_valid.pl")

    val vardict = new VarDictJava(
      tumorBam          = tumorBam,
      bed               = bed,
      ref               = ref,
      tumorName         = tn,
      minimumAf         = minimumAf,
      includeNonPf      = includeNonPf,
      minimumQuality    = minimumQuality,
      maximumMismatches = maximumMismatches,
      minThreads        = minThreads,
      maxThreads        = maxThreads
    )
    val bias               = new ShellCommand(biasScript.toString) with Pipe[Any,Any]
    val toVcf              = new ShellCommand(vcfScript.toString, "-N", tn, "-E", "-f", minimumAf.toString) with Pipe[Any,Vcf]
    val removeRefEqAltRows = new ShellCommand("awk", "{if ($1 ~ /^#/) print; else if ($4 != $5) print}") with Pipe[Vcf,Vcf]
    val sortVcf            = new SortVcf(dict=Some(dict))

    bias.requires(Cores(0), Memory("32m"))
    toVcf.requires(Cores(0), Memory("32m"))

    root ==> (vardict | bias | toVcf | removeRefEqAltRows | sortVcf > out).withName(this.getClass.getSimpleName)
  }
}
