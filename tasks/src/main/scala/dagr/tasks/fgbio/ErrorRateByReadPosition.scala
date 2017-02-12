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

import dagr.tasks.DagrDef.{PathPrefix, PathToBam, PathToFasta, PathToIntervals, PathToVcf}

import scala.collection.mutable.ListBuffer

case class ErrorRateByReadPosition(in: PathToBam,
                                   out: Option[PathPrefix] = None,
                                   ref: PathToFasta,
                                   variants: Option[PathToVcf],
                                   intervals: Option[PathToIntervals],
                                   includeDuplicates: Option[Boolean] = None,
                                   minMappingQuality: Option[Int] = None,
                                   minBaseQuality: Option[Int] = None
                             ) extends FgBioTask {

  override protected def addFgBioArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("-i", in)
    out.foreach(buffer.append("-o", _))
    buffer.append("-r", ref)
    variants.foreach(buffer.append("-v", _))
    intervals.foreach(buffer.append("-l", _))
    includeDuplicates.foreach(buffer.append("-d", _))
    minMappingQuality.foreach(buffer.append("-m", _))
    minBaseQuality.foreach(buffer.append("-q", _))
  }
}
