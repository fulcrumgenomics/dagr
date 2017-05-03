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

import java.nio.file.Path

import dagr.tasks.DagrDef.{DirPath, FilePath, PathPrefix}

import scala.collection.mutable.ListBuffer

object CollectIlluminaBasecallingMetrics {
  def toMetricsExtension(lane: Int): String = s".lane-$lane.metrics"
}

/** The output metric file will be named "<prefix>.lane-<lane>.metrics.txt" and will be placed in the
  * same directory as [[prefix]] if given, otherwise it will be named "basecalling.lane-<lane>.metrics.txt" and be
  * placed in [[basecallsDir]].
  */
class CollectIlluminaBasecallingMetrics(basecallsDir: DirPath,
                                        lane: Int,
                                        barcodeFile: Option[FilePath] = None,
                                        readStructure: String,
                                        override val prefix: Option[PathPrefix] = None,
                                        barcodesDir: Option[DirPath] = None
                                       ) extends PicardTask with PicardMetricsTask {

  /** The path to the input file used to name the output metrics file if [[prefix]] is not given.  In our case, the
    * input is the basecalling directory and not a file.  Therefore, we set [[in]] to be a non-existent file with name
    * "basecalling" located in the basecalls directory.  If [[prefix]] is given, then [[in]] will not be used.
    */
  override def in: FilePath = basecallsDir.resolve("basecalling")

  override def metricsExtension: String = CollectIlluminaBasecallingMetrics.toMetricsExtension(lane=lane)

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer += "BASECALLS_DIR=" + basecallsDir
    barcodesDir.foreach(dir => buffer += "BARCODES_DIR=" + dir)
    buffer += "LANE=" + lane
    barcodeFile.foreach(buffer += "INPUT=" + _)
    buffer += "READ_STRUCTURE=" + readStructure
    buffer += "OUTPUT=" + metricsFile
  }
}
