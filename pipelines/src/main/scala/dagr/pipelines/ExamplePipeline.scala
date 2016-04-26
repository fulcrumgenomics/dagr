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
package dagr.pipelines

import java.nio.file.Files

import dagr.commons.io.Io
import dagr.core.cmdline.Pipelines
import dagr.core.tasksystem.Pipeline
import dagr.sopt.{arg, clp}
import dagr.tasks.DagrDef
import dagr.tasks.bwa.BwaMem
import dagr.tasks.picard.{CollectHsMetrics, DeleteBam, MarkDuplicates, SortSam}
import DagrDef.{DirPath, PathToFasta, PathToFastq, PathToIntervals}
import htsjdk.samtools.SAMFileHeader.SortOrder

/**
  * Very simple example pipeline that is used in the README.md
  */
@clp(description="Example FASTQ to BAM pipeline.", group = classOf[Pipelines])
class ExamplePipeline
( @arg(flag="i", doc="Input fastq.")        val fastq: PathToFastq,
  @arg(flag="r", doc="Reference fasta.")    val ref: PathToFasta,
  @arg(flag="t", doc="Target regions.")     val targets: Option[PathToIntervals] = None,
  @arg(flag="o", doc="Output directory.")   val out: DirPath,
  @arg(flag="p", doc="Output file prefix.") val prefix: String
) extends Pipeline(Some(out)) {

  override def build(): Unit = {
    val bam    = out.resolve(prefix + ".bam")
    val tmpBam = out.resolve(prefix + ".tmp.bam")
    val metricsPrefix: Some[DirPath] = Some(out.resolve(prefix))
    Files.createDirectories(out)

    val bwa   = new BwaMem(fastq=fastq, ref=ref)
    val sort  = new SortSam(in=Io.StdIn, out=tmpBam, sortOrder=SortOrder.coordinate)
    val mark  = new MarkDuplicates(inputs=Seq(tmpBam), out=Some(bam), prefix=metricsPrefix)
    val rmtmp = new DeleteBam(tmpBam)

    root ==> (bwa | sort) ==> mark ==> rmtmp
    targets.foreach(path => root ==> new CollectHsMetrics(in=bam, prefix=metricsPrefix, targets=path, ref=ref))
  }
}
