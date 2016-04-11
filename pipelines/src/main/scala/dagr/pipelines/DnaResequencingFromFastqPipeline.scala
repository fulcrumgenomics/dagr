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
import dagr.core.tasksystem._
import dagr.commons.io.Io
import dagr.sopt.cmdline.ValidationException
import dagr.tasks.DagrDef
import DagrDef._
import dagr.tasks.picard._
import dagr.sopt._

import scala.collection.mutable.ListBuffer

object DnaResequencingFromFastqPipeline {
  final val SUMMARY =
    """
      |Dna Resequencing from Fastq Pipeline.
      |  - FastqToSam -> MarkIlluminaAdapters -> Unmapped BAM
      |  - Unmapped BAM -> SamToFastq -> Bwa Mem -> MergeBamAlignment -> MarkDuplicates -> Mapped BAM
      |  - Mapped BAM -> {CollectMultipleMetrics, EstimateLibraryComplexity, ValidateSamFile}
      |  - Mapped BAM -> {CalculateHsMetrics, CollectTargetedPcrMetrics} if targets are given
      |  - Mapped BAM -> {CollectWgsMetrics, CollectGcBiasMetrics} if targets are not given
    """
}

@clp(
  description = DnaResequencingFromFastqPipeline.SUMMARY,
  group = classOf[Pipelines])
class DnaResequencingFromFastqPipeline
( @arg(doc="Input fastq file (optionally gzipped) for read 1.")    val fastq1: List[PathToFastq],
  @arg(doc="Input fastq file (optionally gzipped) for read 2.")    val fastq2: List[PathToFastq],
  @arg(doc="Path to the reference FASTA.")                         val ref: PathToFasta,
  @arg(flag="s", doc="The name of the sample.")                    val sample: String,
  @arg(flag="l", doc="The name of the library.")                   val library: String,
  @arg(flag="p", doc="The platform unit (@RG.PU).")                val platformUnit: List[String],
  @arg(doc="Use bwa aln/sampe' instead of bwa mem.")               val useBwaBacktrack: Boolean = false,
  @arg(doc="The number of threads to use for BWA.")                val numBwaMemThreads: Int = 3,
  @arg(doc="The number of reads to target when downsampling.")     val downsampleToReads: Long = Math.round(185e6 / 101),
  @arg(flag="t", doc="Target intervals to run HsMetrics over.")    val targetIntervals: Option[PathToIntervals],
  @arg(doc="Path to a temporary directory.")                       val tmp: Path,
  @arg(flag="o", doc="The output directory to which files are written.")  val out: DirPath,
  @arg(doc="The basename for all output files. Uses library if omitted.") val basename: Option[FilenamePrefix]
) extends Pipeline(outputDirectory = Some(out), suffix=Some("." + library)) {
  name = getClass.getSimpleName

  // Validation logic as constructor code
  var errors: ListBuffer[String] = new ListBuffer[String]()
  if (fastq1.size != fastq2.size)       errors += "fastq1 and fastq2 must be specified the same number of times"
  if (fastq1.size != platformUnit.size) errors += "fastq1 and platformUnit must be specified the same number of times"
  if (errors.nonEmpty) throw new ValidationException(errors)

  /**
    * Main method that constructs all the tasks in the pipeline and wires their dependencies together.
    */
  override def build(): Unit = {
    val base = basename.getOrElse(library)
    val prefix = out.resolve(base)

    Io.assertReadable(fastq1 ++ fastq2)
    Io.assertReadable(ref)
    if (targetIntervals.isDefined) Io.assertReadable(targetIntervals.get)
    Io.assertCanWriteFile(prefix, parentMustExist=false)
    Files.createDirectories(out)

    val unmappedBam = Files.createTempFile(tmp, "unmapped.", ".bam")
    val inputs = (fastq1, fastq2, platformUnit).zipped
    val fastqToBams = ListBuffer[ProcessTask]()
    val unmappedBams = ListBuffer[PathToBam]()

    ///////////////////////////////////////////////////////////////////////
    // Create an unmapped BAM
    ///////////////////////////////////////////////////////////////////////
    val prepareUnmappedBam = new CreateUnmappedBamFromFastqPipeline(
      fastq1=fastq1,
      fastq2=fastq2,
      sample=sample,
      library=library,
      platformUnit=platformUnit,
      tmp=tmp,
      out=out,
      unmappedBam=Some(unmappedBam),
      basename=Some(base)
    )

    ///////////////////////////////////////////////////////////////////////
    // Run the rest of the pipeline
    ///////////////////////////////////////////////////////////////////////
    val unmappedBamToMappedBamPipeline: DnaResequencingFromUnmappedBamPipeline = new DnaResequencingFromUnmappedBamPipeline(
      unmappedBam=unmappedBam,
      ref=ref,
      useBwaBacktrack=useBwaBacktrack,
      downsampleToReads=downsampleToReads,
      targetIntervals=targetIntervals,
      tmp=tmp,
      out=out,
      basename=base
    )

    root ==> prepareUnmappedBam ==> unmappedBamToMappedBamPipeline ==> new DeleteBam(unmappedBam)
  }
}
