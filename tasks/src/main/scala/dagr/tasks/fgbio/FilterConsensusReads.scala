/*
 * The MIT License
 *
 * Copyright (c) 2017 Fulcrum Genomics LLC
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

package dagr.tasks.fgbio

import com.fulcrumgenomics.commons.CommonsDef.PathToFasta
import dagr.tasks.DagrDef.PathToBam

import scala.collection.mutable.ListBuffer

object FilterConsensusReads {
  def apply(in: PathToBam,
            out: PathToBam,
            ref: PathToFasta,
            minReads: Int,
            maxReadErrorRate: Double,
            maxBaseErrorRate: Double,
            minQuality: Int,
            maxNoCallFraction: Double,
            minMeanBaseQuality: Option[Int],
            requireSingleStrandAgreement: Option[Boolean]): FilterConsensusReads = {
    apply(in=in, out=out, ref=ref, minReads=Seq(minReads), maxReadErrorRate=Seq(maxReadErrorRate),
      maxBaseErrorRate=Seq(maxBaseErrorRate), minQuality=minQuality, maxNoCallFraction=maxNoCallFraction,
      minMeanBaseQuality=minMeanBaseQuality, requireSingleStrandAgreement=requireSingleStrandAgreement)
  }
}

case class FilterConsensusReads(in: PathToBam,
                                out: PathToBam,
                                ref: PathToFasta,
                                minReads: Seq[Int],
                                maxReadErrorRate: Seq[Double],
                                maxBaseErrorRate: Seq[Double],
                                minQuality: Int,
                                maxNoCallFraction: Double,
                                minMeanBaseQuality: Option[Int] = None,
                                requireSingleStrandAgreement: Option[Boolean] = None)
  extends FgBioTask {

  override protected def addFgBioArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("-i", in)
    buffer.append("-o", out)
    buffer.append("-r", ref)
    if (minReads.nonEmpty)         { buffer.append("-M"); buffer.append(minReads:_*)         }
    if (maxReadErrorRate.nonEmpty) { buffer.append("-E"); buffer.append(maxReadErrorRate:_*) }
    if (maxBaseErrorRate.nonEmpty) { buffer.append("-e"); buffer.append(maxBaseErrorRate:_*) }
    buffer.append("-N", minQuality)
    buffer.append("-n", maxNoCallFraction)
    minMeanBaseQuality.foreach { q => buffer.append("-q", q) }
    requireSingleStrandAgreement.foreach { s => buffer.append("-s", s) }
  }
}
