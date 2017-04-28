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
 *
 */

package dagr.tasks.picard

import dagr.tasks.DagrDef.DirPath

import scala.collection.mutable.ListBuffer

/**
  * CollectIlluminaLaneMetrics does not support read structures with molecular barcodes (M) or skips (S).  If a read
  * structure is provided with either, an error will be thrown.  The preferred solution is to not specify a
  * read structure so that CollectIlluminaLaneMetrics will use the RunInfo.xml in the run directory.
  */
class CollectIlluminaLaneMetrics(runDirectory: DirPath,
                                 output: DirPath,
                                 flowcellBarcode: String,
                                 readStructure: Option[String] = None
                                ) extends PicardTask {

  if (readStructure.exists(_.exists(c => c == 'M' || c == 'S'))) {
      throw new IllegalArgumentException(s"Found 'M' or 'S' segment in read structure '$readStructure'.")
  }

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer += "RUN_DIRECTORY=" + runDirectory
    buffer += "OUTPUT_DIRECTORY=" + output
    buffer += "OUTPUT_PREFIX=" + flowcellBarcode
    readStructure.foreach(rs => buffer += "READ_STRUCTURE=" + rs)
  }
}
