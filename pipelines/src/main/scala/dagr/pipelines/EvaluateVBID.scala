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

import com.fulcrumgenomics.commons.io.PathUtil
import com.fulcrumgenomics.sopt.{arg, clp}
import dagr.core.cmdline.Pipelines
import dagr.core.config.Configuration
import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.{EitherTask, FixedResources, Linker, NoOpInJvmTask, Pipeline, ProcessTask}
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
class EvaluateVBID
(@arg(flag = 'i', doc = "Input bams to evaluate.") val bams: Seq[PathToBam],
 @arg(flag = 'o', doc = "Output directory.") val out: DirPath,
 @arg(flag = 'p', doc = "Output file prefix.") val prefix: String,
 @arg(flag = 'm', doc = "Number of markers to use.") val markers: Seq[Integer],
 @arg(flag = 'R', doc = "VBID vcf resource.") val vbidResource: PathToVcf,

) extends Pipeline(Some(out)) {

  override def build(): Unit = {

    val countVariants = new CountVariants(vbidResource, out.resolve(prefix + ".vbid.count"))
    root ==> countVariants

    bams.foreach(bam => {
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
          bam = bam,
          out = out.resolve(PathUtil.replaceExtension(bam, s".${markerCount}_markers.selfSM"))
        )

        countVariants ==>
          Linker(countVariants, downsample)((f, c) => c.downsampleProportion = markerCount / f.count) ==>
          vbid
      }
      }

    })

  }
}
