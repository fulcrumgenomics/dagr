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
  * Runs the GATK haplotype caller i GVCF mode on a single sample.
  */
class HaplotypeCaller(ref: PathToFasta,
                      intervals: PathToIntervals,
                      val bam: PathToBam,
                      val vcf: PathToVcf,
                      val maxAlternateAlleles: Int = 3,
                      val contaminationFraction: Double = 0.0,
                      val rnaMode: Boolean = false
                      )
 extends GatkTask("HaplotypeCaller", ref, intervals=Some(intervals)) {

  override protected def addWalkerArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("--minPruning", "3")
    buffer.append("--maxNumHaplotypesInPopulation", "200")
    buffer.append("-variant_index_parameter", "128000")
    buffer.append("-variant_index_type", "LINEAR")
    buffer.append("--emitRefConfidence", "GVCF")

    buffer.append("--max_alternate_alleles", maxAlternateAlleles)
    buffer.append("--contamination_fraction_to_filter", contaminationFraction)
    buffer.append("-I", bam.toAbsolutePath)
    buffer.append("-o", vcf.toAbsolutePath)

    if (rnaMode) {
      buffer.append("-dontUseSoftClippedBases")
      buffer.append("-stand_call_conf", 20)
      buffer.append("-stand_emit_conf", 20)
    }
  }
}
