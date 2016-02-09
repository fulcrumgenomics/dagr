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
import dagr.core.tasksystem.{FixedResources, ProcessTask}
import dagr.tasks._

import scala.collection.mutable.ListBuffer

object FilterFreeBayesCalls {

  val VcfLibBinConfigKey   = "vcflib.bin"
  val VtBinConfigKey       = "vt.bin"
  val BcfToolsBinConfigKey = "bcftools.bin"
  val BgzipBinConfigKey    = "bgzip.bin"

  protected val PipeArg = "\\\n  |"
}

/** Applies various filters and ensures proper VCF formatting for variants produced by FreeBayes.
  *
  * This application of tools follows the procedure in bcbio's use of FreeBayes:
  *   https://github.com/chapmanb/bcbio-nextgen/blob/master/bcbio/variation/freebayes.py
  * Briefly, does the following in order:
  * - fix ambiguous (IUPAC) reference base calls
  * - remove alternate alleles that are not called in any sample
  * - split MNP variants into multiple records
  * - updates AC and NS
  * - sort records using a fixed length window
  * - standardizes the representation of variants using parsimony and left-alignment relative to the reference genome*
  * - remove both duplicate and reference alternate alleles
  *
  * * See: http://genome.sph.umich.edu/wiki/Variant_Normalization
  *
  *   * Tool Requirements:
  * - vcflib    (https://github.com/vcflib/vcflib
  * - vt        (https://github.com/atks/vt)
  * - bcftools  (https://samtools.github.io/bcftools/)
  * - bgzip     (https://github.com/samtools/htslib)
  *
  * You can set the path to the various scripts and executables within the configuration, or make sure they are
  * on your PATH.
  *
  * */
class FilterFreeBayesCalls(val in: PathToVcf,
                           val out: PathToVcf,
                           val ref: PathToFasta,
                           val compress: Boolean = true,
                           val somatic: Boolean = false,
                           val minQual: Int = 5
                          )
  extends ProcessTask with Configuration with FixedResources {
  import FilterFreeBayesCalls._

  requires(1.0, "2G")

  override def args: Seq[Any] = {
    val buffer = ListBuffer[Any]()

    // De-compress the input VCF
    buffer.append(configureExecutableFromBinDirectory(BgzipBinConfigKey, "bgzip"), "-c", "-d", in)

    // Remove low quality calls
    buffer.append(PipeArg, configureExecutableFromBinDirectory(VcfLibBinConfigKey, "vcffilter"), "-f", s"'QUAL > $minQual'", "-s")

    // Apply filters specific to somatic calling
    if (somatic) {
      // source: https://github.com/chapmanb/bcbio-nextgen/blob/60a0f3c4f8ec658f8c34d6bc77b23a90f47b45d6/bcbio/variation/freebayes.py#L250
      // TODO
      logger.warning("bcbio filtering, for example, adding REJECTS or SOMATIC flags, has not been implemented.  Contact your nearest developer.")
    }

    // fix ambiguous (IUPAC) reference base calls
    // source: https://github.com/chapmanb/bcbio-nextgen/blob/e4d520b23a4868b8d92f83a3bc0ac11894cd2dbc/bcbio/variation/vcfutils.py#L86
    buffer.append(PipeArg, """awk -F$'\t' -v OFS='\t' '{if ($0 !~ /^#/) gsub(/[KMRYSWBVHDX]/, "N", $4) } { print }'""")

    // remove alternate alleles that are not called in any sample
    buffer.append(PipeArg, configureExecutableFromBinDirectory(BcfToolsBinConfigKey, "bcftools"), "view", "-a", "-", "2> /dev/null")

    // remove calls with missing alternative alleles
    // source: https://github.com/chapmanb/bcbio-nextgen/blob/60a0f3c4f8ec658f8c34d6bc77b23a90f47b45d6/bcbio/variation/freebayes.py#L237
    buffer.append(PipeArg, """awk -F$'\t' -v OFS='\t' '{if ($0 ~ /^#/ || $4 != ".") { print } }'""")


    // split MNP variants into multiple records
    buffer.append(PipeArg, configureExecutableFromBinDirectory(VcfLibBinConfigKey, "vcfallelicprimitives"), "--keep-geno")

    // update AC and NS
    buffer.append(PipeArg, configureExecutableFromBinDirectory(VcfLibBinConfigKey, "vcffixup"), "-")

    // sort records using a fixed length window
    buffer.append(PipeArg, configureExecutableFromBinDirectory(VcfLibBinConfigKey, "vcfstreamsort"))

    // standardizes the representation of variants using parsimony and left-alignment relative to the reference genome
    // See: http://genome.sph.umich.edu/wiki/Variant_Normalization
    buffer.append(PipeArg, configureExecutableFromBinDirectory(VtBinConfigKey,     "vt"), "normalize", "-n", "-r", ref, "-q", "-", "2> /dev/null")

    // remove both duplicate and reference alternate alleles
    buffer.append(PipeArg, configureExecutableFromBinDirectory(VcfLibBinConfigKey, "vcfuniqalleles"))

    // Output it
    if (compress) buffer.append(PipeArg, configureExecutableFromBinDirectory(BgzipBinConfigKey, "bgzip"), "-c")
    buffer.append(">", out)

    buffer
  }
}
