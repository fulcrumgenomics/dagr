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

import java.text.SimpleDateFormat

import dagr.api.models.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.{JvmRanOutOfMemory, VariableResources}
import dagr.tasks.DagrDef.{DirPath, FilePath}
import htsjdk.samtools.util.Iso8601Date
import picard.util.IlluminaUtil.IlluminaAdapterPair

import scala.collection.mutable.ListBuffer

class IlluminaBasecallsToSam(basecallsDir: DirPath,
                             lane: Int,
                             runBarcode: String,
                             readStructure: String,
                             libraryParamsFile: FilePath,
                             runDate: Option[Iso8601Date] = None,
                             sequencingCenter: Option[String] = None,
                             includeNonPfReads: Boolean = false,
                             minThreads: Int = 4,
                             maxThreads: Int = 16,
                             adapterPairs: Seq[IlluminaAdapterPair] = Seq(
                               IlluminaAdapterPair.INDEXED,
                               IlluminaAdapterPair.DUAL_INDEXED,
                               IlluminaAdapterPair.NEXTERA_V2,
                               IlluminaAdapterPair.FLUIDIGM
                             ),
                             barcodesDir: Option[DirPath] = None,
                             maxReadsInRamPerTile: Option[Int] = Some(500000),
                             tmpDir: Option[DirPath] = None
                            ) extends PicardTask with VariableResources with JvmRanOutOfMemory {

  protected val byMemoryPerThread: Memory = Memory("1GB")
  protected var memoryPerThread: Memory = Memory("2GB")

  /** Increases the memory per core/thread and returns true if we can run with the fewest # of threads. */
  override protected def nextMemory(currentMemory: Memory): Option[Memory] = {
    // Increase the amount of memory required per-core
    this.memoryPerThread = this.memoryPerThread + this.byMemoryPerThread
    Some(Memory(this.memoryPerThread.value * minThreads))
  }

  /** Chooses the maximum # of cores given a memory per core requirement. */
  override def pickResources(resources: ResourceSet): Option[ResourceSet] = {
    Range.inclusive(start=maxThreads, end=minThreads, step= -1)
      .flatMap { cores =>
        resources.subset(Cores(cores), Memory(cores * memoryPerThread.value))
      }.headOption
  }

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer += "BASECALLS_DIR=" + basecallsDir
    buffer += "LANE=" + lane
    buffer += "RUN_BARCODE=" + runBarcode
    barcodesDir.foreach(dir => buffer += "BARCODES_DIR=" + dir)
    runDate.foreach(date => buffer += "RUN_START_DATE=" + new SimpleDateFormat("yyyy/MM/dd").format(date))
    buffer += "SEQUENCING_CENTER=" + sequencingCenter.getOrElse("null")
    buffer += "NUM_PROCESSORS=" + resources.cores.toInt
    buffer += "READ_STRUCTURE=" + readStructure.toString
    buffer += "LIBRARY_PARAMS=" + libraryParamsFile
    buffer += "INCLUDE_NON_PF_READS=" + includeNonPfReads
    if (adapterPairs.isEmpty) buffer += "ADAPTERS_TO_CHECK=null"
    else adapterPairs.foreach(buffer += "ADAPTERS_TO_CHECK=" + _)
    maxReadsInRamPerTile.foreach(n => "MAX_READS_IN_RAM_PER_TILE=" + n)
    tmpDir.foreach(tmp => buffer += "TMP_DIR=" + tmp)
  }
}

