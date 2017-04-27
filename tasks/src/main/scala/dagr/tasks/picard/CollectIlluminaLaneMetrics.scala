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

object CollectIlluminaLaneMetrics {
  def convertBarcodeAndSkipToTemplate(readStructure: String): String = {
    readStructure
      .replaceAll("[MS]", "T")
      .split("(?<=[BT])") // split by segment type, but keep it around
      .map(s => (s.dropRight(1).toInt, s.last.toString)) // split into template length and type
      .foldLeft(("", 0, "")) { case ((rs, lastLen, lastSeg), (len, seg)) => // group all of the same type
        if (lastSeg == seg || lastLen == 0) {
          (rs, lastLen  + len, seg)
        } else {
          (s"$rs$lastLen$lastSeg", len, seg)
        }
      }
      .productIterator.mkString // the last segment needs to be joined
  }
}

/**
  * CollectIlluminaLaneMetrics does not support read structures with molecular barcodes (M) or skips (S).  If a read structure
  * is provided with either, an error will be thrown.  The preferred solution is to convert all molecular barcode and
  * skip segments to template (T).
  */
class CollectIlluminaLaneMetrics(runDirectory: DirPath,
                                 output: DirPath,
                                 flowcellBarcode: String,
                                 readStructure: String,
                                 convertBarcodeAndSkipToTemplate: Boolean = true
                                ) extends PicardTask {

  private val _readStructure: String = if (convertBarcodeAndSkipToTemplate) {
    CollectIlluminaLaneMetrics.convertBarcodeAndSkipToTemplate(readStructure=this.readStructure)
  }
  else {
    this.readStructure
  }

  if (readStructure.exists(c => c == 'M' || c == 'S')) {
    throw new IllegalArgumentException(s"Found 'M' or 'S' segment in read structure '$readStructure'.")
  }

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer += "RUN_DIRECTORY=" + runDirectory
    buffer += "OUTPUT_DIRECTORY=" + output
    buffer += "OUTPUT_PREFIX=" + flowcellBarcode
    buffer += "READ_STRUCTURE=" + _readStructure
  }
}
