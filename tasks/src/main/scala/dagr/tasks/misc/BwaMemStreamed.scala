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

import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.DataTypes.{Fastq, Sam}
import dagr.core.tasksystem._
import dagr.core.util.{PathUtil, Io}
import dagr.tasks.picard.{FifoBuffer, MergeBamAlignment, PicardTask, SamToFastq}
import dagr.tasks.{PathToBam, PathToFasta, PipedTask}

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
                    ) extends Pipeline {

  override def build(): Unit = {
    val samToFastq = SamToFastq(in=unmappedBam, out=Io.StdOut).requires(Cores(0.5), Memory(samToFastqMem))
    val fqBuffer   = new FifoBuffer[Fastq].requires(memory=Memory(fifoBufferMem))
    val bwaMem     = new BwaMem(fastq = Io.StdIn, ref = ref, minThreads = minThreads, maxThreads = maxThreads, memory = Memory(bwaMemMemory))
    val bwaBuffer  = new FifoBuffer[Sam].requires(memory=Memory(fifoBufferMem))

    var pipe = samToFastq | fqBuffer | bwaMem | bwaBuffer

    // Optionally tie in alt-processing if requested and the .alt file exists
    val alt = PathUtil.pathTo(ref + ".alt")
    if (processAltMappings && alt.toFile.exists()) {
      val k8       = new BwaK8AltProcessor(altFile = alt)
      val k8Buffer = new FifoBuffer[Sam].requires(memory=Memory(fifoBufferMem))
      pipe = pipe | k8 | k8Buffer
    }

    val mergeBam = new MergeBamAlignment(unmapped=unmappedBam, mapped=Io.StdIn, out=mappedBam, ref=ref).requires(memory=Memory(mergeBamAlignmentMem))
    root ==> (pipe | mergeBam).withName("BwaMemThroughMergeBam")
  }
}