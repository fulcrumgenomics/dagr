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
package dagr.tasks.picard

import java.nio.file.Path

import dagr.tasks.DagrDef
import DagrDef.{PathToIntervals, PathToVcf}

import scala.collection.mutable.ListBuffer

/** Runs Picard's GenotypeConcordance to assess various variant calling metrics against truth.
  *
  * Please note MISSING_SITES_HOM_REF is set to true by default, whereas the default in GenotypeConcordance is false. */
class GenotypeConcordance(val truthVcf: PathToVcf,
                         val callVcf: PathToVcf,
                         var truthSample: String,
                         var callSample: String,
                         val prefix: Path,
                         val intervals: List[PathToIntervals] = Nil,
                         val intersectIntervals: Boolean = true,
                         val minGQ: Int = 0,
                         val minDP: Int = 0,
                         val missingSitesAreHomRef: Boolean = true
                         ) extends PicardTask {

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("TRUTH_VCF="             + truthVcf.toAbsolutePath)
    buffer.append("CALL_VCF="              + callVcf.toAbsolutePath)
    buffer.append("TRUTH_SAMPLE="          + truthSample)
    buffer.append("CALL_SAMPLE="           + callSample)
    buffer.append("OUTPUT="                + prefix.toAbsolutePath)
    intervals.foreach(interval => buffer.append("INTERVALS=" + interval.toAbsolutePath))
    buffer.append("INTERSECT_INTERVALS="   + intersectIntervals)
    buffer.append("MIN_GQ="                + minGQ)
    buffer.append("MIN_DP="                + minDP)
    buffer.append("MISSING_SITES_HOM_REF=" + missingSitesAreHomRef)
  }
}
