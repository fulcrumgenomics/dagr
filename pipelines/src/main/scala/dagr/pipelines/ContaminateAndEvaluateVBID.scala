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

import dagr.commons.io.Io
import dagr.core.cmdline.Pipelines
import dagr.core.config.Configuration
import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.{EitherTask, FixedResources, NoOpInJvmTask, Pipeline, ProcessTask}
import dagr.sopt.{arg, clp}
import dagr.tasks.DagrDef._
import dagr.tasks.bwa.BwaMem
import dagr.tasks.gatk.LeftAlignAndTrimVariants
import dagr.tasks.misc.{DWGSim, LinkFile}
import dagr.tasks.picard.{DownsampleSam, DownsamplingStrategy, MergeSamFiles, SortSam}
import htsjdk.samtools.SAMFileHeader.SortOrder

import scala.collection.mutable

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
 @arg(name = "header", doc = "header line for dp.") val dpHeader: Path,
 @arg(name = "dp-bed", doc = "bed file with dp values") val pathToBed: Path

) extends Pipeline(Some(out)) {

  override def build(): Unit = {
    val strat = Some(DownsamplingStrategy.HighAccuracy)

    Files.createDirectories(out)
    val bamYield = new mutable.HashMap[PathToBam, Int]
    val coverage = 100
    val makeBamsLoop = new NoOpInJvmTask("made bams")

    val bams:Seq[PathToBam]=vcfs.map(vcf => {

      val normalize = new LeftAlignAndTrimVariants(in=vcf, out=out.resolve(prefix + vcf.getFileName + ".normalized.vcf"), ref=ref, intervals = Some(targets), splitMultiAlleic = Some(true))

      val rando = new ProcessTask with FixedResources {
        requires(Cores(1), Memory("1G"))

        override val name = "move dp over to vcf"

        val outVcf=out.resolve(prefix + vcf.getFileName + ".normalized.withdp.vcf")
        /**
          * Abstract method that must be implemented by child classes to return a list or similar traversable
          * list of command line elements (command name and arguments) that form the command line to be run.
          * Individual values will be converted to Strings before being used by calling toString.
          */
        override def args: Seq[Any] = "bcftools" :: "annotate" ::
          "-a" :: pathToBed ::
          "-h" :: dpHeader ::
          "-c" :: "CHROM,FROM,TO,dp" ::
          "-O" :: outVcf ::
          normalize.out :: Nil

        /**
          * Given a non-null ResourceSet representing the available resources at this moment in time
          * return either a ResourceSet that is a subset of the available resources in which the task
          * can run, or None if the task cannot run in the available resources.
          *
          * @param availableResources The system resources available to the task
          * @return Either a ResourceSet of the desired subset of resources to run with, or None
          */
        override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = None
      }


      val simulate = new DWGSim(vcf = rando.outVcf, fasta = ref, outPrefix = out.resolve(prefix + vcf.getFileName + ".sim"), depth = coverage, coverageTarget = targets)
      val bwa = new BwaMem(fastq = simulate.outputPairedFastq, ref = ref,maxThreads = 1, memory = Memory("2G"))
      val tmpBam = out.resolve(prefix + vcf.getFileName + ".tmp.bam")
      val sort = new SortSam(in = Io.StdIn, out = tmpBam, sortOrder = SortOrder.coordinate) with Configuration {
        requires(Cores(1), Memory("2G"))
      }

      root ==> normalize ==> rando ==> simulate ==> (bwa | sort) ==> makeBamsLoop

      tmpBam
    })

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
//          markers.foreach { markerCount => {}

            // subset VBID resource files to smaller number of markers

            // call VBID using subsetted markers

//          }

        }
        }
      })

    }
    }
  }
}





