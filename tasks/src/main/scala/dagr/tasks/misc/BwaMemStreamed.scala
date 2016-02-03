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
package dagr.tasks.misc

import java.nio.file.Paths

import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.{VariableResources, FixedResources, ProcessTask, UnitTask}
import dagr.core.util.Io
import dagr.tasks.picard.{PicardTask, FifoBuffer, MergeBamAlignment, SamToFastq}
import dagr.tasks.{PipedTask, PathToBam, PathToFasta}

import scala.collection.mutable.ListBuffer

/**
 *
 * @param unmappedBam the BAM in which to read unmapped data.
 * @param mappedBam the BAM in which to store mapped data
 * @param ref the reference FASTA file, with BWA indexes pre-built
 * @param minThreads the minimum number of threads to allocate to bwa-mem
 * @param maxThreads the maximum number of threads to allocate to bwa-mem
 * @param bwaMemMemory the amount of memory based on the referenceFasta
 * @param samToFastqMem the amount of memory for SamToFastq
 * @param mergeBamAlignmentMem the amount of memory for MergeBamAlignment
 * @param fifoBufferMem the amount of memory for each FifoBuffer
 */
class BwaMemStreamed(unmappedBam: PathToBam,
                     mappedBam: PathToBam,
                     ref: PathToFasta,
                     minThreads: Int = 1,
                     maxThreads: Int = 32,
                     bwaMemMemory: String = "8G",
                     samToFastqMem: String = "512M",
                     mergeBamAlignmentMem: String = "2G",
                     fifoBufferMem: String = "512M",
                     processAltMappings : Boolean = true
                    ) extends PipedTask with VariableResources {
  this.name = "BwaMemStreamed"
  override protected val tasks = ListBuffer[ProcessTask]()

  tasks ++= SamToFastq(in = unmappedBam, out = Io.StdOut).requires(memory = Memory(samToFastqMem)).getTasks
  tasks ++= new FifoBuffer().requires(Cores(0), Memory(fifoBufferMem)).getTasks
  tasks ++= new BwaMem(fastq = Io.StdIn, ref = ref, minThreads = minThreads, maxThreads = maxThreads, memory = Memory(bwaMemMemory)).getTasks
  tasks ++= new FifoBuffer().requires(memory = Memory(fifoBufferMem)).getTasks

  // Optionally tie in alt-processing if requested and the .alt file exists
  {
    val alt = Paths.get(ref + ".alt")
    if (processAltMappings && alt.toFile.exists()) {
      tasks ++= new BwaK8AltProcessor(altFile = alt).getTasks
      tasks ++= new FifoBuffer().requires(memory = Memory(fifoBufferMem)).getTasks
    }
  }

  tasks ++= new MergeBamAlignment(unmapped = unmappedBam, mapped = Io.StdIn, out = mappedBam, ref = ref).requires(memory = Memory(mergeBamAlignmentMem)).getTasks

  /**
    * Attempts to pick the resources required to run. The required resources are:
    * 1) Cores  = the cores for bwa-mem plus one additional core to account for the other programs
    * 2) Memory = the sum of memory required for all processes
    */
  override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
    val totalMemory = this.tasks.map({
      case f: FixedResources => f.resources.memory.bytes
      case _ => 0
    }).sum[Long]

    availableResources.subset(minCores = Cores(minThreads + 1), maxCores = Cores(maxThreads + 1), memory = Memory(totalMemory))
  }

  override def applyResources(resources: ResourceSet): Unit = {
    logger.debug("BWA Streamed schedule resource set: " + resources)

    val bwaResources = ResourceSet(Cores(resources.cores.value - 1), Memory(bwaMemMemory))
    tasks.foreach {
      case t: PicardTask => t.applyResources(t.resources)
      case t: BwaMem => t.applyResources(bwaResources)
      case _ => Unit
    }
  }
}