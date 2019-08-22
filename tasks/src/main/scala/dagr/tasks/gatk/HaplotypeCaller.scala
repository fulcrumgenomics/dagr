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
package dagr.tasks.gatk

import dagr.tasks.DagrDef
import DagrDef.{PathToBam, PathToFasta, PathToIntervals, PathToVcf}

import scala.collection.mutable.ListBuffer

/**
  * Runs the GATK haplotype caller in GVCF mode on a single sample.
  */
class HaplotypeCaller(ref: PathToFasta,
                      intervals: Option[PathToIntervals],
                      val bam: PathToBam,
                      val vcf: PathToVcf,
                      val maxAlternateAlleles: Int = 3,
                      val minPruning: Int = 3,
                      val contaminationFraction: Double = 0.0,
                      val rnaMode: Boolean = false,
                      val useNativePairHmm: Boolean = false,
                      val ploidy: Option[Int] = None,
                      val maxReadsInRegionPerSample: Option[Int] = None,
                      val minReadsPerAlignmentStart: Option[Int] = None
                      )
 extends GatkTask("HaplotypeCaller", ref, intervals=intervals) {

  override protected def addWalkerArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("-I", bam.toAbsolutePath)
    buffer.append("-o", vcf.toAbsolutePath)
    buffer.append(either("--minPruning", "--min-pruning"), minPruning)
    buffer.append(either("--maxNumHaplotypesInPopulation", "--max-num-haplotypes-in-population"), "200")
    buffer.append(either("--emitRefConfidence", "--emit-ref-confidence"), "GVCF")
    buffer.append(either("--max_alternate_alleles", "--max-alternate-alleles"), maxAlternateAlleles)
    buffer.append(either("--contamination_fraction_to_filter", "--contamination-fraction-to-filter"), contaminationFraction)
    ploidy.foreach(p => buffer.append(either("--sample_ploidy", "--sample-ploidy"), p))
    if (useNativePairHmm) buffer.append("-pairHMM", either("VECTOR_LOGLESS_CACHING", "FASTEST_AVAILABLE"))

    if (gatkMajorVersion < 4) {
      buffer.append("-variant_index_parameter", "128000")
      buffer.append("-variant_index_type", "LINEAR")
      maxReadsInRegionPerSample.foreach(n => buffer.append("--maxReadsInRegionPerSample", n))
      minReadsPerAlignmentStart.foreach(n => buffer.append("--minReadsPerAlignmentStart", n))
    }

    if (rnaMode) {
      buffer.append(either("-dontUseSoftClippedBases", "--dont-use-soft-clipped-bases"))
      buffer.append(either("-stand_call_conf", "-stand-call-conf"), 20)
      if (gatkMajorVersion < 4) buffer.append("-stand_emit_conf", 20)
    }
  }
}
