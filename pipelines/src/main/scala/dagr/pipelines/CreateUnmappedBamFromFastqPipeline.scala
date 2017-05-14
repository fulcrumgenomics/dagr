/*
 * The MIT License
 *
 * Copyright (c) 2015-2016 Fulcrum Genomics LLC
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

import java.nio.file.{Files, Path}

import dagr.core.cmdline._
import dagr.core.tasksystem.{Pipeline, ProcessTask, ShellCommand}
import com.fulcrumgenomics.commons.io.Io
import com.fulcrumgenomics.sopt.cmdline.ValidationException
import dagr.tasks.DagrDef
import DagrDef._
import dagr.tasks.picard.{FastqToUnmappedSam, MergeSamFiles, DeleteBam}
import com.fulcrumgenomics.sopt._
import htsjdk.samtools.SAMFileHeader.SortOrder
import htsjdk.samtools.util.FastqQualityFormat

import scala.collection.mutable.ListBuffer

object CreateUnmappedBamFromFastqPipeline {
  final val SUMMARY =
    """
      |Create Unmapped Bam From Fastq Pipeline.  Runs:
      |  - FastqToSam -> MarkIlluminaAdapters -> Unmapped BAM
      |"""
}

@clp(
  description = CreateUnmappedBamFromFastqPipeline.SUMMARY,
  group = classOf[Pipelines])
class CreateUnmappedBamFromFastqPipeline
( @arg(doc="Input fastq file (optionally gzipped) for read 1.")                   val fastq1: List[PathToFastq],
  @arg(doc="Input fastq file (optionally gzipped) for read 2.")                   val fastq2: List[PathToFastq],
  @arg(flag='s', doc="The name of the sample.")                                   val sample: String,
  @arg(flag='l', doc="The name of the library.")                                  val library: String,
  @arg(flag='p', doc="The platform unit (@RG.PU).")                               val platformUnit: List[String],
  @arg(doc="Path to a temporary directory.")                                      val tmp: Path,
  @arg(flag='o', doc="The output directory in which to write files.")             val out: DirPath,
  @arg(doc="The filename prefix for output files. Library is used if omitted.")   val basename: Option[FilenamePrefix] = None,
  @arg(doc="Path to the unmapped BAM. Use the output prefix if none is given. ")  var unmappedBam: Option[PathToBam] = None,
  @arg(doc="""
             |A value describing how the quality values are encoded in the input FASTQ file.
             |Either Solexa (phred scaling + 66), Illumina (phred scaling + 64) or Standard " +
             |(phred scaling 33).  If this value is not specified, the quality format will be " +
             |detected automatically."
            """)
                                                                                  val qualityFormat: Option[FastqQualityFormat] = None
) extends Pipeline(Some(out)) {

  name = "CreateUnmappedBamFromFastqPipeline"

  // Validation logic as constructor code
  var errors: ListBuffer[String] = new ListBuffer[String]()
  if (fastq1.size != fastq2.size)       errors += "fastq1 and fastq2 must be specified the same number of times"
  if (fastq1.size != platformUnit.size) errors += "fastq1 and platformUnit must be specified the same number of times"
  if (errors.nonEmpty) throw new ValidationException(errors)

  /**
    * Main method that constructs all the tasks in the pipeline and wires their dependencies together.
    */
  override def build(): Unit = {
    val prefix: String = basename.getOrElse(library)

    Io.assertReadable(fastq1 ++ fastq2)
    Files.createDirectories(out)
    Io.assertCanWriteFile(out.resolve(prefix), parentMustExist=false)

    val unmappedBamFile = unmappedBam.getOrElse(out.resolve(prefix + ".bam"))
    val inputs          = (fastq1, fastq2, platformUnit).zipped
    val fastqToBams     = ListBuffer[ProcessTask]()
    val unmappedBams    = ListBuffer[PathToBam]()

    ///////////////////////////////////////////////////////////////////////
    // Generate an unmapped BAM for each pair of fastqs input
    ///////////////////////////////////////////////////////////////////////
    inputs.foreach((fq1, fq2, pu) => {
      val bam = Files.createTempFile(tmp, "unmapped.", ".bam")
      unmappedBams += bam

      val fastqToSam = FastqToUnmappedSam(fq1=fq1, fq2=fq2, bam=bam, sm=sample, lb=library, pu=pu, prefix=Some(out.resolve(prefix)), qualityFormat=qualityFormat)
      fastqToBams += fastqToSam
      root ==> fastqToSam
    })

    ///////////////////////////////////////////////////////////////////////
    // Then either merge all the input BAMs, or if we have just a single
    // one, then just rename it
    ///////////////////////////////////////////////////////////////////////
    unmappedBams.toList match {
      case uBam :: Nil =>
        fastqToBams.head ==> ShellCommand("mv", uBam.toString, unmappedBamFile.toString).withName("Move unmapped BAM")
      case _ =>
        val mergeUnmappedSams = new MergeSamFiles(in=unmappedBams, out=unmappedBamFile, sortOrder=SortOrder.queryname)
        fastqToBams.foreach(_ ==> mergeUnmappedSams)
        unmappedBams.foreach(b => mergeUnmappedSams ==> new DeleteBam(b))
    }
  }
}
