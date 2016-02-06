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
import dagr.core.util.Io
import dagr.tasks.picard._
import dagr.tasks._

import scala.collection.mutable.ListBuffer

object DnaResequencingFromFastqPipeline {
  @inline
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

@CLP(
  description = DnaResequencingFromFastqPipeline.SUMMARY,
  group = classOf[Pipelines])
class DnaResequencingFromFastqPipeline(
  @Arg(doc="Input fastq file (optionally gzipped) for read 1.")    val fastq1: List[PathToFastq],
  @Arg(doc="Input fastq file (optionally gzipped) for read 2.")    val fastq2: List[PathToFastq],
  @Arg(doc="Path to the reference FASTA.")                         val referenceFasta: PathToFasta,
  @Arg(flag="s", doc="The name of the sample.")                    val sample: String,
  @Arg(flag="l", doc="The name of the library.")                   val library: String,
  @Arg(flag="p", doc="The platform unit (@RG.PU).")                val platformUnit: List[String],
  @Arg(doc="Use bwa aln/sampe' instead of bwa mem.")               val useBwaBacktrack: Boolean = false,
  @Arg(doc="The number of threads to use for BWA.")                val numBwaMemThreads: Int = 3,
  @Arg(doc="The number of reads to target when downsampling.")     val downsampleToReads: Long = Math.round(185e6 / 101),
  @Arg(flag="t", doc="Target intervals to run HsMetrics over.")    val targetIntervals: Option[PathToIntervals],
  @Arg(doc="Path to a temporary directory.")                       val tmp: Path,
  @Arg(flag="o", doc="The output directory to which files are written.")  val output: DirPath,
  @Arg(doc="The basename for all output files. Uses library if omitted.") val basename: Option[FilenamePrefix]
) extends Pipeline(outputDirectory = Some(output)) {
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
    val prefix = output.resolve(base)

    Io.assertReadable(fastq1 ++ fastq2)
    Io.assertReadable(referenceFasta)
    if (targetIntervals.isDefined) Io.assertReadable(targetIntervals.get)
    Io.assertCanWriteFile(prefix, parentMustExist=false)
    Files.createDirectories(output)

    val unmappedBam = Files.createTempFile(tmp, "unmapped.", ".bam")
    val inputs = (fastq1, fastq2, platformUnit).zipped
    val fastqToBams = ListBuffer[FastqToUnmappedSam]()
    val unmappedBams = ListBuffer[PathToBam]()

    ///////////////////////////////////////////////////////////////////////
    // Create an unmapped BAM
    ///////////////////////////////////////////////////////////////////////
    val prepareUnmappedBam = new CreateUnmappedBamFromFastqPipeline(
      fastq1=fastq1,
      fastq2=fastq2,
      referenceFasta=referenceFasta,
      sample=sample,
      library=library,
      platformUnit=platformUnit,
      tmp=tmp,
      output=output,
      unmappedBam=Some(unmappedBam),
      basename=Some(base)
    )

    ///////////////////////////////////////////////////////////////////////
    // Run the rest of the pipeline
    ///////////////////////////////////////////////////////////////////////
    val unmappedBamToMappedBamPipeline: DnaResequencingFromUnmappedBamPipeline = new DnaResequencingFromUnmappedBamPipeline(
      unmappedBam=unmappedBam,
      referenceFasta=referenceFasta,
      useBwaBacktrack=useBwaBacktrack,
      downsampleToReads=downsampleToReads,
      targetIntervals=targetIntervals,
      tmp=tmp,
      output=output,
      basename=base
    )

    root ==> prepareUnmappedBam ==> unmappedBamToMappedBamPipeline ==> new RemoveBam(unmappedBam)
  }
}
