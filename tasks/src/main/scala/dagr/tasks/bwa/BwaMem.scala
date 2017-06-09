/*
 * The MIT License
 *
 * Copyright (c) 2015 Fulcrum Genomics LLC
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
package dagr.tasks.bwa

import dagr.core.tasksystem.{Pipe, ProcessTask, VariableResources}
import com.fulcrumgenomics.commons.io.Io
import dagr.tasks.DataTypes.{Fastq, Sam}
import dagr.tasks.DagrDef
import DagrDef.{PathToBam, PathToFasta, PathToFastq}
import dagr.core.exec.{Cores, Memory, ResourceSet}

import scala.collection.mutable.ListBuffer

class BwaMem(fastq: PathToFastq = Io.StdIn,
             out: Option[PathToBam] = None,
             ref: PathToFasta,
             minSeedLength: Option[Int] = None,
             matchScore: Option[Int] = None,
             mismatchPenalty: Option[Int] = None,
             gapOpenPenalties: Option[(Int,Int)] = None,
             gapExtensionPenalties: Option[(Int,Int)] = None,
             clippingPenalties: Option[(Int,Int)] = None,
             minScore: Option[Int] = None,
             smartPairing: Boolean = true,
             minThreads: Int = 1,
             maxThreads: Int = 32,
             memory: Memory = Memory("8G")
             ) extends ProcessTask with VariableResources with Pipe[Fastq,Sam] {
  name = "BwaMem"

  override def pickResources(resources: ResourceSet): Option[ResourceSet] = {
    resources.subset(minCores=Cores(minThreads), maxCores=Cores(maxThreads), memory=memory)
  }

  override def args: Seq[Any] = {
    val buffer = ListBuffer[Any](Bwa.findBwa, "mem", "-t", resources.cores.toInt)

    if (smartPairing) buffer.append("-p")
    minSeedLength.foreach(l => buffer.append("-k", l))
    matchScore.foreach(s => buffer.append("-A", s))
    mismatchPenalty.foreach(p => buffer.append("-B", p))
    gapOpenPenalties.foreach      { case (del, ins) => buffer.append("-O", s"$del,$ins") }
    gapExtensionPenalties.foreach { case (del, ins) => buffer.append("-E", s"$del,$ins") }
    clippingPenalties.foreach { case (five, three) => buffer.append("-L", s"$five,$three") }
    minScore.foreach(s => buffer.append("-T", s))

    buffer.append(ref, fastq)
    out.foreach(f => buffer.append("> " + f))

    buffer.toList
  }
}
