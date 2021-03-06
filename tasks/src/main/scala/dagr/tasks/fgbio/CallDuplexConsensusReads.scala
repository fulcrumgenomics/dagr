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

import dagr.core.execsystem.{Cores, ResourceSet}
import dagr.core.tasksystem.{Pipe, VariableResources}
import dagr.tasks.DagrDef.PathToBam
import dagr.tasks.DataTypes.SamOrBam

import scala.collection.mutable.ListBuffer

class CallDuplexConsensusReads(val in: PathToBam,
                               val out: PathToBam,
                               val readNamePrefix:      Option[String] = None,
                               val readGroupId:         Option[String] = None,
                               val errorRatePreUmi:     Option[Int]    = None,
                               val errorRatePostUmi:    Option[Int]    = None,
                               val minInputBaseQuality: Option[Int]    = None,
                               val minReads:            Seq[Int]       = Seq.empty,
                               val maxReadsPerStrand:   Option[Int]    = None,
                               val minThreads: Int                     = 1,
                               val maxThreads: Int                     = 32
                              ) extends FgBioTask with VariableResources with Pipe[SamOrBam,SamOrBam] {

  override def pickResources(resources: ResourceSet): Option[ResourceSet] = {
    resources.subset(minCores=Cores(minThreads), maxCores=Cores(maxThreads), memory=this.resources.memory)
  }

  override protected def addFgBioArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("-i", in)
    buffer.append("-o", out)
    readNamePrefix.foreach      (x => buffer.append("-p", x))
    readGroupId.foreach         (x => buffer.append("-R", x))
    errorRatePreUmi.foreach     (x => buffer.append("-1", x))
    errorRatePostUmi.foreach    (x => buffer.append("-2", x))
    minInputBaseQuality.foreach (x => buffer.append("-m", x))
    if (minReads.nonEmpty) {
      buffer.append("-M")
      buffer.append(minReads:_*)
    }
    maxReadsPerStrand.foreach   (x => buffer.append("--max-reads-per-strand", x))
    buffer.append("--threads", resources.cores.toInt)
  }
}
