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

import java.nio.file.{Files, Path}

import dagr.commons.io.{Io, PathUtil}
import dagr.core.cmdline.Pipelines
import dagr.core.config.Configuration
import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.{EitherTask, FixedResources, Linker, NoOpInJvmTask, Pipeline, ProcessTask}
import dagr.sopt.{arg, clp}
import dagr.tasks.DagrDef._
import dagr.tasks.bwa.BwaMem
import dagr.tasks.gatk.{CountVariants, DownsampleVariants, Gatk4Task, GatkTask, IndexVariants, LeftAlignAndTrimVariants, VariantsToTable}
import dagr.tasks.misc.{DWGSim, LinkFile, VerifyBamId}
import dagr.tasks.picard.{DownsampleSam, DownsamplingStrategy, MergeSamFiles, SortSam}
import htsjdk.samtools.SAMFileHeader.SortOrder

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Downsample Bams to create composite contaminated bams,
  * then downsample to various depths
  * and run VBID with different number of target snps to evaluate
  * dependance of VBID on contamination & depth & number of target snps
  */
@clp(description = "Example FASTQ to BAM pipeline.", group = classOf[Pipelines])
class ContaminateAndEvaluateVBID
(@arg(flag = "i", doc = "Input vcfs for generating bams.") val vcfs: Seq[PathToVcf],
 @arg(flag = "r", doc = "Reference fasta.") val ref: PathToFasta,
 @arg(flag = "o", doc = "Output directory.") val out: DirPath,
 @arg(flag = "t", doc = "Target regions.") val targets: PathToIntervals,
 @arg(flag = "p", doc = "Output file prefix.") val prefix: String,
 @arg(flag = "c", doc = "Contamination levels to evaluate.") val contaminations: Seq[Double],
 @arg(flag = "d", doc = "Depth values to evaluate.") val depths: Seq[Integer],
 @arg(flag = "m", doc = "Number of markers to use.") val markers: Seq[Integer],
 @arg(flag = "R", doc = "VBID vcf resource.") val vbidResource: PathToVcf,
 @arg(flag = "H", doc = "header line for dp.") val dpHeader: Path

) extends Pipeline(Some(out)) {

  override def build(): Unit = {
    val strat = Some(DownsamplingStrategy.HighAccuracy)

    Files.createDirectories(out)
    val bamYield = new mutable.HashMap[PathToBam, Int]
    val coverage = 100
    val makeBamsLoop = new NoOpInJvmTask("made bams")

    val bams: Seq[PathToBam] = vcfs.map(vcf => {

      val toTable = new VariantsToTable(in = vcf, out = out.resolve(prefix + vcf.getFileName + ".table"), fields = "CHROM" :: "POS" :: "HET" :: "HOM-VAR" :: Nil, intervals = Some(targets))
      val makeBed = new MakePLBed(table = toTable.out, out = out.resolve(prefix + "PL.bed"))

      val rando = new CopyPS_FromBedToVcf(in = vcf, out = out.resolve(prefix + vcf.getFileName + ".with.pl.vcf"),
        dpHeader = dpHeader, pathToBed = makeBed.out)

      val subsetToPL = new subsetToPL(in=rando.out, out=out.resolve(prefix + vcf.getFileName + ".subsetToPL.vcf"))
      val normalize = new LeftAlignAndTrimVariants(in = subsetToPL.out, out = out.resolve(prefix + vcf.getFileName + ".normalized.vcf"), ref = ref, splitMultiAlleic = Some(true))
      val index = new IndexVariants(in = normalize.out)
      val simulate = new DWGSim(vcf = normalize.out, fasta = ref, outPrefix = out.resolve(prefix + vcf.getFileName + ".sim"), depth = coverage, coverageTarget = targets)
      val bwa = new BwaMem(fastq = simulate.outputPairedFastq, out=Some(out.resolve(prefix + vcf.getFileName + ".tmp.bam")) ,
        ref = ref, maxThreads = 1, memory = Memory("2G"))
      val sort = new SortSam(in = bwa.out.get, out = out.resolve(prefix + vcf.getFileName + ".sorted.bam"), sortOrder = SortOrder.coordinate) with Configuration {
        requires(Cores(1), Memory("2G"))
      }

      root ==> toTable ==> makeBed ==> rando ==> subsetToPL ==>
        normalize ==> index ==> simulate ==> bwa ==> sort ==> makeBamsLoop


      sort.out
    })

    val countVariants = new CountVariants(vbidResource, out.resolve(prefix + ".vbid.count"))
    root ==> countVariants

    val pairsOfBam = for (x <- bams; y <- bams) yield (x, y)

    pairsOfBam.foreach { case (x, y) => {
      contaminations.foreach(c => {
        val dsX = out.resolve(prefix + x.getFileName + ".forCont." + c + ".tmp.bam")
        val dsY = out.resolve(prefix + y.getFileName + ".forCont." + c + ".tmp.bam")
        // create a mixture of c x + (1-c) y taking into account the depths of
        // x and y
        // D_x and D_y are the effective depths of x and y
        // if we are to downsample x we need x to be downsampled to c/(1-c)
        // if we are to downsample y we need y to be downsampled to (1-c)/c
        // of of those two values will be less than 1.
        val downsampleX = new DownsampleSam(in = x, out = dsX, proportion = c / (1 - c), strategy = strat).requires(Cores(1), Memory("2g"))
        val downsampleY = new DownsampleSam(in = x, out = dsY, proportion = (1 - c) / c, strategy = strat).requires(Cores(1), Memory("2g"))

        val copyX = new LinkFile(x, dsX)
        val copyY = new LinkFile(y, dsY)
        val downsampleX_Q = c / (1 - c) <= 1
        val downsample = EitherTask.of(
          downsampleX :: copyY :: Nil,
          downsampleY :: copyX :: Nil,
          downsampleX_Q)

        val xFactor = if (downsampleX_Q) c / (1 - c) else 1
        val yFactor = if (downsampleX_Q) 1 else (1 - c) / c

        val resultantDepth: Double = bamYield.getOrElse(x, 0) * xFactor + bamYield.getOrElse(y, 0) * yFactor

        val xyContaminatedBam = out.resolve(prefix + "__" + x.getFileName + "__" + y.getFileName +
          "__" + c + ".bam")
        val mergedownsampled = new MergeSamFiles(in = dsX :: dsY :: Nil, out = xyContaminatedBam, sortOrder = SortOrder.coordinate)

        makeBamsLoop ==> downsample ==> mergedownsampled

        depths filter (d => d <= resultantDepth) foreach { d => {
          val outDownsampled = out.resolve(prefix + "__" + x.getFileName + "__" + y.getFileName +
            "__" + c + "__target__" + d + ".bam")
          val downsampleMerged = new DownsampleSam(in = xyContaminatedBam, out = outDownsampled, proportion = d / resultantDepth)

          mergedownsampled ==> downsampleMerged

          markers.foreach { markerCount => {
            // subset VBID resource files to smaller number of markers

            // this need to happen after the tool is run...need to figure out how to do that
            val outDownsampledResource = out.resolve(prefix + "__" + vbidResource.getFileName +
              "__" + markerCount + ".vcf")

            val downsample = new DownsampleVariants(
              in = vbidResource,
              output = outDownsampledResource,
              proportion = 1)

            val vbid = new VerifyBamId(
              vcf = outDownsampledResource,
              bam = xyContaminatedBam,
              out = out.resolve(PathUtil.replaceExtension(xyContaminatedBam, s".${markerCount}_markers.selfSM"))
            )

            (downsampleMerged :: countVariants) ==>
              Linker(countVariants, downsample)((f, c) => c.downsampleProportion = markerCount / f.count) ==>
              vbid
          }
          }
        }
        }
      })
    }
    }
  }
}

// copies the value from the bed file to the vcf
// removed the PS field (since it's invalid)
private class CopyPS_FromBedToVcf(val in: PathToVcf,
                                  val out: PathToVcf,
                                  val pathToBed: Path,
                                  val dpHeader: Path
                                 ) extends ProcessTask with FixedResources with Configuration {
  requires(Cores(1), Memory("1G"))

  private val bcftools: Path = configureExecutable("bcftools.executable", "bcftools")

  override def args: Seq[Any] = bcftools :: "annotate" ::
    "-a" :: pathToBed ::
    "-h" :: dpHeader ::
    "-c" :: "CHROM,FROM,TO,pl" ::
    "-x" :: "FORMAT" :: // remove the FORMAT Field
    "-x" :: "^INFO/pl" :: // no need for all these annotations anyway
    "--force" :: // needed since the input file is corrupt.
    "-o" :: out ::
    in :: Nil
}


// copies the value from the bed file to the vcf
// removed the PS field (since it's invalid)
private class subsetToPL(val in: PathToVcf, val out: Path
                        ) extends ProcessTask with FixedResources with Configuration {
  requires(Cores(1), Memory("1G"))

  private val bcftools: Path = configureExecutable("bcftools.executable", "bcftools")

  override def args: Seq[Any] = bcftools :: "view" ::
    "-i" :: "pl!=\".\"" ::
    "-o" :: out ::
    in :: Nil
}


// copies the value from the bed file to the vcf
private class MakePLBed(val table: Path,
                        val out: Path
                       ) extends ProcessTask with FixedResources with Configuration {
  requires(Cores(1), Memory("1G"))

  private val awk: Path = configureExecutable("awk.executable", "awk")

  quoteIfNecessary = false

  override def args: Seq[Any] = awk :: raw"""'BEGIN{OFS="\t"; } NR>1{print $$1,$$2-1, $$2, $$3+2*$$4}'""" ::
    table.toAbsolutePath :: ">" :: out.toAbsolutePath :: Nil
}



