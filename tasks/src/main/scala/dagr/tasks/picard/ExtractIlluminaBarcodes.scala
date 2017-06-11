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

import dagr.core.exec.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.VariableResources
import dagr.tasks.DagrDef.{DirPath, FilePath, PathPrefix}

import scala.collection.mutable.ListBuffer

object ExtractIlluminaBarcodes {
  def toMetricsExtension(lane: Int): String = s".lane-$lane.metrics"
}

/** The output metric file will be named "<prefix>.lane-<lane>.metrics.txt" and will be placed in the
  * same directory as [[prefix]] if given, otherwise it will be named "barcode_counts.lane-<lane>.metrics.txt" and be
  * placed in [[basecallsDir]].
  */
class ExtractIlluminaBarcodes(basecallsDir: DirPath,
                              lane: Int,
                              readStructure: String,
                              barcodeFile: FilePath,
                              maxMismatches: Option[Int] = None,
                              minMismatchDelta: Option[Int] = None,
                              maxNoCalls: Option[Int] = None,
                              minBaseQuality: Option[Int] = None,
                              compressOutputs: Boolean = true,
                              minThreads: Int = 4,
                              maxThreads: Int = 16,
                              override val prefix: Option[PathPrefix] = None
                             ) extends PicardTask with PicardMetricsTask with VariableResources {

  private val _barcodesDir: DirPath = prefix.map(_.getParent).getOrElse(basecallsDir)

  override def pickResources(resources: ResourceSet): Option[ResourceSet] = {
    resources.subset(minCores=Cores(minThreads), maxCores=Cores(maxThreads), memory=Memory("4G"))
  }

  /** The path to the input file used to name the output metrics file if [[prefix]] is not given.  In our case, the
    * input is the basecalling directory and not a file.  Therefore, we set [[in]] to be a non-existent file with name
    * "barcode_counts" located in the basecalls directory.  If [[prefix]] is given, then [[in]] will not be used.
    */
  override def in: FilePath = basecallsDir.resolve("barcode_counts")

  override def metricsExtension: String = ExtractIlluminaBarcodes.toMetricsExtension(lane)

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer += "BASECALLS_DIR=" + basecallsDir
    buffer += "OUTPUT_DIR=" + _barcodesDir
    buffer += "LANE=" + lane
    buffer += "READ_STRUCTURE=" + readStructure.toString
    buffer += "BARCODE_FILE=" + barcodeFile
    buffer += "METRICS_FILE=" + metricsFile
    buffer += "COMPRESS_OUTPUTS=" + compressOutputs
    buffer += "NUM_PROCESSORS=" + resources.cores.toInt
    maxMismatches.foreach(buffer += "MAX_MISMATCHES=" + _)
    minMismatchDelta.foreach(buffer += "MIN_MISMATCH_DELTA=" + _)
    maxNoCalls.foreach(buffer += "MAX_NO_CALLS=" + _)
    minBaseQuality.foreach(buffer += "MINIMUM_BASE_QUALITY=" + _)
  }
}
