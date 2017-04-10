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

import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.VariableResources
import dagr.tasks.DagrDef.{DirPath, FilePath}

import scala.collection.mutable.ListBuffer


/** The output metric file will be named "barcode_counts.lane-<lane>.metrics.txt".  And will be placed in the
  * [[output]] if given, otherwise the [[basecallsDir]].
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
                              output: Option[DirPath] = None
                             ) extends PicardTask with PicardMetricsTask with VariableResources {

  private val _barcodesDir: DirPath = output.getOrElse(basecallsDir)

  override def pickResources(resources: ResourceSet): Option[ResourceSet] = {
    resources.subset(minCores=Cores(minThreads), maxCores=Cores(maxThreads), memory=Memory("4G"))
  }

  override def in: FilePath = _barcodesDir.resolve("barcode_counts")

  override def metricsExtension: String = s".lane-$lane.metrics"

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
