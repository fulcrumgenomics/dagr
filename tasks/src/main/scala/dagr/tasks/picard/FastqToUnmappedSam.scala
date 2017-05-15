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

import dagr.core.tasksystem.Pipe
import com.fulcrumgenomics.commons.io.Io
import dagr.tasks.DataTypes.SamOrBam
import dagr.tasks.DagrDef
import DagrDef.{PathToBam, PathToFastq}
import htsjdk.samtools.util.FastqQualityFormat

object FastqToUnmappedSam {
  /**
    * Constructs a new set of piped tasks for taking PE Illumina data from two fastq files and producing
    * an unampped BAM file in which the possible location of adapter sequence in each read is marked.
    *
    * @param fq1 the read one fastq file (can be gzipped)
    * @param fq2 the read two fastq file (can be gzipped)
    * @param bam the output SAM or BAM file to write to
    * @param sm the name of the sample to put in the read group header
    * @param lb the name of the library to put in the read group header
    * @param pu the platform unit string to put in the tread group header
    * @param rgId the optional ID to use in the read group header. If None one will be generated.
    * @param prefix the prefix (including directories) to use to write metrics and ancillary files
    * @return a Pipe from Nothing (since the inputs are read from file) to SamOrBam
    */
  def apply(fq1: PathToFastq, fq2: PathToFastq, bam: PathToBam, sm: String, lb: String, pu: String, rgId: Option[String] = None, prefix: Option[Path], qualityFormat: Option[FastqQualityFormat] = None): Pipe[Nothing,SamOrBam] = {
    val fastqToSam = new FastqToSam(fastq1=fq1, fastq2=Some(fq2), out=Io.StdOut, sample=sm, library=Some(lb), readGroupName=rgId, platformUnit=Some(pu), qualityFormat = qualityFormat).withCompression(0)
    val buffer = new FifoBuffer[SamOrBam]
    val markAdapters = new MarkIlluminaAdapters(in=Io.StdIn, out=bam, prefix=prefix)
    (fastqToSam | buffer | markAdapters).withName("FastqToAdapaterMarkedSam").asInstanceOf[Pipe[Nothing,SamOrBam]]
  }
}
