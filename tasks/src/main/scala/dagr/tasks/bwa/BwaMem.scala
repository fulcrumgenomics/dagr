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

import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.{Pipe, ProcessTask, VariableResources}
import dagr.commons.io.Io
import dagr.tasks.DataTypes.{Fastq, Sam}
import dagr.tasks.DagrDef
import DagrDef.{PathToBam, PathToFasta, PathToFastq}

import scala.collection.mutable.ListBuffer

class BwaMem(fastq: PathToFastq = Io.StdIn,
             out: Option[PathToBam] = None,
             ref: PathToFasta,
             minThreads: Int = 1,
             maxThreads: Int = 32,
             memory: Memory = Memory("8G"),
             smartPairing: Boolean = true) extends ProcessTask with VariableResources with Pipe[Fastq,Sam] {
  name = "BwaMem"

  override def pickResources(resources: ResourceSet): Option[ResourceSet] = {
    resources.subset(minCores=Cores(minThreads), maxCores=Cores(maxThreads), memory=memory)
  }

  override def args: Seq[Any] = {
    val buffer = new ListBuffer[Any]()
    buffer.append(Bwa.findBwa)
    buffer.append("mem")
    if (smartPairing) buffer.append("-p")
    buffer.append("-t",  resources.cores.toInt)
    buffer.append(ref, fastq)
    out.foreach { f => buffer.append("> " + f) }
    buffer.toList
  }
}
