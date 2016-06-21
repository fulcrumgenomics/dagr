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
import dagr.core.execsystem.ResourceSet
import dagr.core.tasksystem._
import dagr.tasks.DagrDef
import dagr.tasks.DagrDef._
import dagr.tasks.DataTypes._

object FilterFreeBayesCalls {

  val VcfLibBinConfigKey   = "vcflib.bin"
  val VtBinConfigKey       = "vt.bin"
  val BcfToolsBinConfigKey = "bcftools.bin"
  val BgzipBinConfigKey    = "bgzip.bin"
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
  extends Task with Configuration with FixedResources {
  import FilterFreeBayesCalls._

  requires(1.0, "2G")

  override def applyResources(resources: ResourceSet): Unit = Unit

  override def getTasks: Traversable[_ <: Task] = {
    // Apply filters specific to somatic calling
    if (somatic) {
      // source: https://github.com/chapmanb/bcbio-nextgen/blob/60a0f3c4f8ec658f8c34d6bc77b23a90f47b45d6/bcbio/variation/freebayes.py#L250
      logger.warning("bcbio filtering, for example, adding REJECTS or SOMATIC flags, has not been implemented.  Contact your nearest developer.")
    }

    // De-compress the input VCF
    val decompressVcf = new ShellCommand(configureExecutableFromBinDirectory(BgzipBinConfigKey, "bgzip").toString, "-c", "-d", in.toAbsolutePath.toString) with PipeWithNoResources[Nothing, Vcf]

    // Remove low quality calls
    val filterLowQualityCalls = new ShellCommandAsIs(configureExecutableFromBinDirectory(BcfToolsBinConfigKey, "bcftools").toString, "filter", "-i", """'ALT="<*>" || QUAL > 5'""", "2>", "/dev/null") with PipeWithNoResources[Vcf, Vcf]

    // fix ambiguous (IUPAC) reference base calls
    val fixAmbiguousIupacReferenceCalls = new ShellCommandAsIs("""awk -F$'\t' -v OFS='\t' '{if ($0 !~ /^#/) gsub(/[KMRYSWBVHDX]/, "N", $4) } { print }'""") with PipeWithNoResources[Vcf, Vcf]

    // remove alternate alleles that are not called in any sample
    val removeAlts = new ShellCommandAsIs(configureExecutableFromBinDirectory(BcfToolsBinConfigKey, "bcftools").toString, "view", "-a", "-", "2>", "/dev/null") with PipeWithNoResources[Vcf, Vcf]

    // remove calls with missing alternative alleles
    // source: https://github.com/chapmanb/bcbio-nextgen/blob/60a0f3c4f8ec658f8c34d6bc77b23a90f47b45d6/bcbio/variation/freebayes.py#L237
    val removeMissingAlts = new ShellCommandAsIs("""awk -F$'\t' -v OFS='\t' '{if ($0 ~ /^#/ || $4 != ".") { print } }'""") with PipeWithNoResources[Vcf, Vcf]

    // split MNP variants into multiple records
    val splitMnpVariants = new ShellCommand(configureExecutableFromBinDirectory(VcfLibBinConfigKey, "vcfallelicprimitives").toString, "-t", "DECOMPOSED", "--keep-geno") with PipeWithNoResources[Vcf, Vcf]

    // update AC and NS
    val updateAcAndNs = new ShellCommand(configureExecutableFromBinDirectory(VcfLibBinConfigKey, "vcffixup").toString) with PipeWithNoResources[Vcf, Vcf]

    // sort records using a fixed length window
    val sortVcf = new ShellCommand(configureExecutableFromBinDirectory(VcfLibBinConfigKey, "vcfstreamsort").toString) with PipeWithNoResources[Vcf, Vcf]

    // standardizes the representation of variants using parsimony and left-alignment relative to the reference genome
    // See: http://genome.sph.umich.edu/wiki/Variant_Normalization
    val leftAlign = new ShellCommandAsIs(configureExecutableFromBinDirectory(VtBinConfigKey, "vt").toString, "normalize", "-n", "-r", ref.toAbsolutePath.toString, "-q", "-", "2> /dev/null") with PipeWithNoResources[Vcf, Vcf]

    // remove both duplicate and reference alternate alleles
    val removeDuplicateAlleles = new ShellCommand(configureExecutableFromBinDirectory(VcfLibBinConfigKey, "vcfuniqalleles").toString) with PipeWithNoResources[Vcf, Vcf]

    // compress the output (or not)
    val compressOutput = if (compress) new ShellCommand(configureExecutableFromBinDirectory(BgzipBinConfigKey, "bgzip").toString, "-c") with PipeWithNoResources[Vcf,Vcf] else Pipes.empty[Vcf]

    List(decompressVcf | filterLowQualityCalls | fixAmbiguousIupacReferenceCalls | removeAlts | removeMissingAlts | splitMnpVariants | updateAcAndNs |sortVcf | leftAlign | removeDuplicateAlleles | compressOutput > out)
  }
}
