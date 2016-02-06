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
package dagr.tasks.picard

import java.nio.file.Path

import dagr.core.tasksystem.DataTypes.SamOrBam
import dagr.core.tasksystem.{Pipeline, FixedResources, ProcessTask}
import dagr.core.util.Io
import dagr.tasks.{PipedTask, PathToBam, PathToFastq}

object FastqToUnmappedSam {
  /** Constructs a new FastqToUnmappedSam for PE Illumina data from two fastq files. */
  def apply(fq1: PathToFastq, fq2: PathToFastq, bam: PathToBam,
            sm: String, lb: String, pu: String,
            prefix: Option[Path],
            rgId: Option[String] = None) = {

    new FastqToUnmappedSam(fastq1=fq1, fastq2=Some(fq2), bam=bam, prefix = prefix,
      sampleName = sm, library=Some(lb), platformUnit = Some(pu), readGroupName=rgId).withName("FastqToUnmappedSam." + pu)

  }
}

class FastqToUnmappedSam(fastq1: PathToFastq,
                         fastq2: Option[PathToFastq] = None,
                         bam: PathToBam,
                         sampleName: String,
                         prefix: Option[Path],
                         platform: Option[String] = Some("ILLUMINA"),
                         platformUnit: Option[String] = None,
                         library: Option[String] = None,
                         readGroupName: Option[String] = None,
                         useSequentialFastqs: Boolean = false
                          ) extends Pipeline {

  override def build(): Unit = {
    val fastqToSam: FastqToSam = new FastqToSam(
      name = "FastqToSam",
      fastq1 = fastq1,
      fastq2 = fastq2,
      out = Io.StdOut,
      sampleName = sampleName,
      platform = platform,
      platformUnit = platformUnit,
      library = library,
      readGroupName = readGroupName,
      useSequentialFastqs = useSequentialFastqs)

    val buffer = new FifoBuffer[SamOrBam]
    val markIlluminaAdapters = new MarkIlluminaAdapters(in = Io.StdIn, out = bam, prefix = prefix)
    root ==> (fastqToSam | buffer | markIlluminaAdapters).withName("FastqToAdapaterMarkedSam")
  }
}
