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

package dagr.tasks.vc

import dagr.core.config.Configuration
import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.{ProcessTask, VariableResources}
import dagr.tasks.jeanluc.GenerateRegionsFromFasta
import dagr.tasks.vc.FreeBayes._
import dagr.tasks.{PathToBam, PathToFasta, PathToIntervals, PathToVcf}

import scala.collection.mutable.ListBuffer

object FreeBayes {

  /** FreeBayes configuration */
  val FreeBayesExecutableConfigKey = "freebayes.executable"
  val VcfLibBinConfigKey           = "vcflib.bin"
  val BgzipBinConfigKey            = "bgzip.bin"

  /** FreeBayes defaults */
  val DefaultMemory = Memory("8G")
  val DefaultMinThreads = 1
  val DefaultMaxThreads = 32
  val DefaultRegionSize = 1000000
  val DefaultUseBestNAlleles = Some(4)
  val DefaultMaxCoverage = None //Some(1000)
  val DefaultMinRepeatEntropy = Some(1)

  /** Somatic Defaults */
  val DefaultMinAlternateAlleleFraction = Some(0.1.toFloat)

  protected val PipeArg = "\\\n  |"
}

/**
  * Task for running FreeBayes, based on:
  *   https://github.com/chapmanb/bcbio-nextgen/blob/master/bcbio/variation/freebayes.py
  * Does no pre-filtering for regions with high depth, but uses the `--max-coverage` option instead.  Furthermore,
  * no post-filtering of the calls are performed; see [[FilterFreeBayesCalls]] for applying post-calling filters.
  *
  * `regionSize` will only be used if no interval list is given.
  *
  * Tool Requirements:
  * - freebayes (https://github.com/ekg/freebayes)
  * - vcflib    (https://github.com/vcflib/vcflib
  * - bgzip     (https://github.com/samtools/htslib)
  * - parallel  (http://www.gnu.org/software/parallel)
  *
  * Other Requirements
  * - reference FASTA must have an FAI
  *
  * You can set the path to the various scripts and executables within the configuration, or make sure they are
  * on your PATH.
  *
  * Please note that if the output VCF is a bgzip'ed VCF (vcf.gz) no index will be generated.
  */
abstract class FreeBayes(val ref: PathToFasta,
                         val targetIntervals: Option[PathToIntervals],
                         val bam: List[PathToBam],
                         val vcf: PathToVcf,
                         val somatic: Boolean,
                         val compress: Boolean,
                         val memory: Memory,
                         val minThreads: Int,
                         val maxThreads: Int,
                         val regionSize: Long,
                         val useBestNAlleles: Option[Int],
                         val maxCoverage: Option[Int],
                         val minRepeatEntropy: Option[Int],
                         minAlternateAlleleFraction: Option[Float] = None) extends ProcessTask with Configuration with VariableResources {

  // NB: we could re-implement the FreeBayes Parallel script as individual tasks, but keep this for now.
  protected def numThreads = (this.resources.cores.value - 1).toInt

  if (!somatic && minAlternateAlleleFraction.isDefined) {
    throw new IllegalArgumentException("Somatic calling but minAlternateAlleleFraction is defined")
  }

  /** Attempts to pick the resources required to run. The required resources are:
    * 1) Cores  = the cores for freebayes plus one additional core to account for the other scripts
    * 2) Memory = a fixed amount.
    */
  override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
    availableResources.subset(minCores = Cores(minThreads + 1), maxCores = Cores(maxThreads + 1), memory = memory)
  }

  /** Sets the arguments for FreeBayes.  For somatic calling, there should be two bams, a tumor bam then normal bam. */
  override def args: Seq[Any] = {
    val buffer = ListBuffer[Any]()

    /** Trivial method to make the adding of arguments to the list of arguments more readable below. */
    def applyArgs[T](arg: String, value: Option[T]): Unit = value.foreach(buffer.append(arg, _))

    // Generates the regions on the fly
    if (targetIntervals.isEmpty) { // Split using the FASTA
      new GenerateRegionsFromFasta(ref=ref, regionSize=Some(regionSize.toInt)).getTasks.foreach(task => buffer.append(task.args))
    }
    else { // Split using the intervals
      buffer.append("cat " + targetIntervals.get.toAbsolutePath + """ | grep -v ^@ | awk '{printf("%s:%d-%d\n", $1, $2-1, $3);}'""")
    }

    buffer.append(PipeArg, s"parallel -k -j $numThreads")

    // Runs FreeBayes on multiple regions in parallel.
    buffer.append(configureExecutable(FreeBayesExecutableConfigKey, "freebayes"))

    // FreeBayes args
    buffer.append("-f", ref)
    applyArgs("--use-best-n-alleles",     useBestNAlleles)
    applyArgs("--max-coverage",           maxCoverage)
    applyArgs("--min-repeat-entropy",     minRepeatEntropy)
    applyArgs("--min-alternate-fraction", minAlternateAlleleFraction)
    if (somatic) {
      buffer.append("--pooled-discrete", "--pooled-continuous", "--genotype-qualities")
      buffer.append("--report-genotype-likelihood-max", "--allele-balance-priors-off")
    }
    bam.foreach(b => buffer.append("-b", b.toAbsolutePath))
    buffer.append("--region {}")

    // Make a header, sort it, and uniq it (for edge regions)
    buffer.append(PipeArg, configureExecutableFromBinDirectory(VcfLibBinConfigKey, "vcffirstheader"))
    buffer.append(PipeArg, configureExecutableFromBinDirectory(VcfLibBinConfigKey, "vcfstreamsort"), "-w", "1000")
    buffer.append(PipeArg, configureExecutableFromBinDirectory(VcfLibBinConfigKey, "vcfuniq"))

    // Output it
    if (compress) buffer.append(PipeArg, configureExecutableFromBinDirectory(BgzipBinConfigKey, "bgzip"), "-c")
    buffer.append(">", vcf)
    buffer
  }
}

/** Performs Germline variant calling using FreeBayes according the the BCBIO best Practices */
class FreeBayesGermline(ref: PathToFasta,
                        targetIntervals: Option[PathToIntervals],
                        bam: List[PathToBam],
                        vcf: PathToVcf,
                        compress: Boolean             = true,
                        memory: Memory                = DefaultMemory,
                        minThreads: Int               = DefaultMinThreads,
                        maxThreads: Int               = DefaultMaxThreads,
                        regionSize: Long              = DefaultRegionSize,
                        useBestNAlleles: Option[Int]  = DefaultUseBestNAlleles,
                        maxCoverage: Option[Int]      = DefaultMaxCoverage,
                        minRepeatEntropy: Option[Int] = DefaultMinRepeatEntropy
) extends FreeBayes(ref, targetIntervals, bam, vcf, false, compress, memory, minThreads, maxThreads,
  regionSize, useBestNAlleles, maxCoverage, minRepeatEntropy)

/** Performs Somatic (Tumor/Normal) variant calling using FreeBayes according the the BCBIO best Practices */
class FreeBayesSomatic(ref: PathToFasta,
                       targetIntervals: Option[PathToIntervals],
                       tumorBam: PathToBam,
                       normalBam: PathToBam,
                       vcf: PathToVcf,
                       compress: Boolean                             = true,
                       memory: Memory                                = DefaultMemory,
                       minThreads: Int                               = DefaultMinThreads,
                       maxThreads: Int                               = DefaultMaxThreads,
                       regionSize: Long                              = DefaultRegionSize,
                       useBestNAlleles: Option[Int]                  = DefaultUseBestNAlleles,
                       maxCoverage: Option[Int]                      = DefaultMaxCoverage,
                       minRepeatEntropy: Option[Int]                 = DefaultMinRepeatEntropy,
                       val minAlternateAlleleFraction: Option[Float] = DefaultMinAlternateAlleleFraction
) extends FreeBayes(ref, targetIntervals, List(tumorBam, normalBam), vcf, true, compress, memory, minThreads, maxThreads,
  regionSize, useBestNAlleles, maxCoverage, minRepeatEntropy, minAlternateAlleleFraction)
