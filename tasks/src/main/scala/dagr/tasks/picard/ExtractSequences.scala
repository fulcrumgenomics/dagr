/*
 * The MIT License
 *
 * Copyright (c) 2019 Fulcrum Genomics LLC
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

import dagr.tasks.DagrDef.{PathToFasta, PathToIntervals}

import scala.collection.mutable.ListBuffer

/** Task to run Picard's ExtractSequences. */
class ExtractSequences(
  val intervals: PathToIntervals,
  val output: PathToFasta,
  val ref: PathToFasta,
  val lineLength: Option[Int] = None
  ) extends PicardTask {

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("INTERVAL_LIST=" + intervals)
    buffer.append("REFERENCE_SEQUENCE=" + ref)
    buffer.append("OUTPUT=" + output)
    lineLength.foreach(length => buffer.append("LINE_LENGTH=" + length))
  }
}