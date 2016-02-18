/*
 * The MIT License
 *
 * Copyright (c) 2016 Fulcrum Genomics LLC
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

import java.nio.file.Path

import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.VariableResources
import dagr.tasks.DagrDef
import DagrDef.{PathToIntervals, PathToVcf}

import scala.collection.mutable.ListBuffer

/** Runs Picard's CollectVariantCallingMetrics metrics. */
class CollectVariantCallingMetrics(vcf: PathToVcf,
                                   prefix: Path,
                                   dbsnp: PathToVcf,
                                   intervals: Option[PathToIntervals] = None,
                                   dict: Option[Path] = None,
                                   gvcf: Boolean = false,
                                   minThreads: Int = 1,
                                   maxThreads: Int = 32)
  extends PicardTask with VariableResources {

  private val memory = Memory("4G")

  override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
    availableResources.subset(minCores = Cores(minThreads), maxCores = Cores(maxThreads), memory = memory)
  }

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("INPUT=" + vcf.toAbsolutePath)
    buffer.append("OUTPUT=" + prefix.toAbsolutePath)
    buffer.append("DBSNP=" + dbsnp.toAbsolutePath)
    intervals.foreach(interval => buffer.append("INTERVALS=" + interval.toAbsolutePath))
    dict.foreach(d => buffer.append("SEQUENCE_DICTIONARY=" + d.toAbsolutePath))
    buffer.append("GVCF_INPUT=" + gvcf)
    buffer.append("THREAD_COUNT=" + resources.cores.toInt)
  }
}
