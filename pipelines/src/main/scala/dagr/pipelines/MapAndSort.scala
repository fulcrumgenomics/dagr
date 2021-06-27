/*
 * The MIT License
 *
 * Copyright (c) 2020 Fulcrum Genomics LLC
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
package dagr.pipelines

import java.nio.file.Files

import com.fulcrumgenomics.sopt.{arg, clp}
import dagr.core.cmdline.Pipelines
import dagr.core.config.Configuration
import dagr.core.execsystem.{Cores, Memory}
import dagr.core.tasksystem._
import dagr.tasks.DagrDef._
import dagr.tasks.bwa.BwaMem
import dagr.tasks.picard.SortSam
import htsjdk.samtools.SAMFileHeader.SortOrder

/**
  * generate bams based on input vcfs
  */
@clp(description = "Example FASTQ to BAM pipeline.", group = classOf[Pipelines])
class MapAndSort
(@arg(flag = 'i', doc = "Input fastq for mapping.") val fastqs: Seq[PathToFastq],
 @arg(flag = 'r', doc = "Reference fasta.") val ref: PathToFasta,
 @arg(flag = 'o', doc = "Output directory.") val out: DirPath,
 @arg(flag = 'p', doc = "Output file prefix.") val prefix: String,

) extends Pipeline(Some(out)) {

  override def build(): Unit = {

    Files.createDirectories(out)

    fastqs.foreach(f => {

      val bwa = new myBwaMem(fastq = f, out = Some(out.resolve(prefix + f.getFileName + ".tmp.sam")), ref = ref)
      val sort = new mySortSam(inn = bwa.out.get, outt = out.resolve(prefix + f.getFileName + ".sorted.bam"))

      root ==> bwa ==> sort

    })
  }


  private class mySortSam(val inn: PathToBam, val outt: PathToBam) extends
    SortSam(in = inn, outt, SortOrder.coordinate) with
    Configuration {
    requires(Cores(1), Memory("2G"))
  }

  private class myBwaMem(val fastq: PathToFastq,
                         val out: Option[PathToBam],
                         val ref: PathToFasta) extends
    BwaMem(
      fastq = fastq,
      out = out,
      ref = ref,
      maxThreads = 2,
      memory = Memory("4G"))


}