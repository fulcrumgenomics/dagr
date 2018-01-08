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
import dagr.tasks.DagrDef.{FilePath, PathToBam}

import scala.collection.mutable.ListBuffer

class ClipBam(val input: PathToBam,
              val output: PathToBam,
              val ref: PathToFasta,
              val metrics: Option[FilePath] = None,
              @deprecated("Use clipping-mode instead.", since="0.2.1")
              val softClip: Option[Boolean] = None,
              val clippingMode: Option[String] = None,
              val autoClipAttributes: Option[Boolean] = None,
              val upgradeClipping: Option[Boolean] = None,
              val readOneFivePrime: Option[Int] = None,
              val readOneThreePrime: Option[Int] = None,
              val readTwoFivePrime: Option[Int] = None,
              val readTwoThreePrime: Option[Int] = None,
              val clipOverlappingReads: Option[Boolean] = None
             ) extends FgBioTask {

  require(softClip.isEmpty || clippingMode.isEmpty, "Both softClip and clippingMode cannot both be used.")

  override protected def addFgBioArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("-i", input)
    buffer.append("-o", output)
    buffer.append("-r", ref)
    metrics.foreach           (m => buffer.append("-m", m))
    softClip.foreach          (s => buffer.append("-s", s))
    clippingMode.foreach      (c => buffer.append("-c", c))
    autoClipAttributes.foreach(a => buffer.append("-a", a))
    upgradeClipping.foreach   (u => buffer.append("--upgrade-clipping", u))
    readOneFivePrime.foreach  (b => buffer.append("--read-one-five-prime", b))
    readOneThreePrime.foreach (c => buffer.append("--read-one-three-prime", c))
    readTwoFivePrime.foreach  (d => buffer.append("--read-two-five-prime", d))
    readTwoThreePrime.foreach (e => buffer.append("--read-two-three-prime", e))
    clipOverlappingReads.foreach(o => buffer.append("--clip-overlapping-reads", o))
  }
}

