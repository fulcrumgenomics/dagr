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

import dagr.core.tasksystem.FixedResources
import dagr.tasks.{PathToBam, PathToFasta, PathToIntervals, PathToVcf}

import scala.collection.mutable.ListBuffer

/**
  * Task for running MuTect2 from the GATK 3.5+ distribution.
  */
class Mutect2(val tumorBam: PathToBam,
              val normalBam: PathToBam,
              reference: PathToFasta,
              intervals: PathToIntervals,
              val output: PathToVcf,
              val maxAltAlleleInNormalFraction: Double = 0.03,
              val maxAltAlleleInNormalCount: Int = 2,
              val maxAltAlleleInNormalBqSum: Int = 20
             ) extends GatkTask(walker="MuTect2", reference=reference, intervals=Some(intervals)) with FixedResources {

  override protected def addWalkerArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("-I:tumor",  tumorBam)
    buffer.append("-I:normal", normalBam)
    buffer.append("-o", output)
    buffer.append("--max_alt_allele_in_normal_fraction",    maxAltAlleleInNormalFraction)
    buffer.append("--max_alt_alleles_in_normal_count",      maxAltAlleleInNormalCount)
    buffer.append("--max_alt_alleles_in_normal_qscore_sum", maxAltAlleleInNormalBqSum)
  }
}
