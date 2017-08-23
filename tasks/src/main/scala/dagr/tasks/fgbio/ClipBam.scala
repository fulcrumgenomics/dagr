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

class ClipBam(val input: PathToBam,
              val output: PathToBam,
              val ref: PathToFasta,
              val softClip: Boolean = false,
              val autoClipAttributes: Boolean = false,
              val readOneFivePrime: Int  = 0,
              val readOneThreePrime: Int = 0,
              val readTwoFivePrime: Int  = 0,
              val readTwoThreePrime: Int = 0
             ) extends FgBioTask {

  override protected def addFgBioArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("-i", input)
    buffer.append("-o", output)
    buffer.append("-r", ref)
    buffer.append("-s", softClip)
    buffer.append("-a", autoClipAttributes)
    buffer.append("-b", readOneFivePrime)
    buffer.append("-c", readOneThreePrime)
    buffer.append("-d", readTwoFivePrime)
    buffer.append("-e", readTwoThreePrime)
  }
}

