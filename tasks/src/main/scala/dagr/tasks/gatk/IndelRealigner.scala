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

package dagr.tasks.gatk

import java.nio.file.Files

import com.fulcrumgenomics.commons.io.{Io, PathUtil}
import dagr.core.tasksystem.{Pipeline, SimpleInJvmTask}
import com.fulcrumgenomics.sopt.{arg, clp}
import dagr.tasks.DagrDef._
import dagr.tasks.misc.DeleteFiles

import scala.collection.mutable.ListBuffer

object IndelRealignment {
  // Apply method here since we can't have two constructors in IndelRealignment that take default parameters.
  def apply(bams: Map[PathToBam, PathToBam],
            ref: PathToFasta,
            known: Seq[PathToVcf] = Nil,
            intervals: Option[PathToIntervals] = None,
            bamCompression: Option[Int] = None) = {
    new IndelRealignment(bams=bams, ref=ref, known=known, intervals=intervals, bamCompression=bamCompression)
  }
}

@clp(
  description = "Run GATK indel cleaning on one or more BAMs together. Output BAMs are created in the output directory " +
    "with name = basename(in) + .cleaned. + suffix(in).",
  hidden = true
)
class IndelRealignment(val bams: Map[PathToBam, PathToBam],
                       ref: PathToFasta,
                       val known: Seq[PathToVcf],
                       intervals: Option[PathToIntervals],
                       bamCompression: Option[Int]
                      ) extends Pipeline {

  /** CLP constructor for testing and manual use, since there's no easy way to provide a map on the CLI. */
  def this(@arg(flag = 'i', doc = "One or more input BAM files.") in: Seq[PathToBam],
           @arg(flag = 'o', doc = "Output directory.") out: DirPath,
           @arg(flag = 'r', doc = "Path to the reference fasta.") ref: PathToFasta,
           @arg(flag = 'k', doc = "Zero or more VCFs of known indels.") known: Seq[PathToVcf] = Nil,
           @arg(flag = 'l', doc = "Optional regions to run over") intervals: Option[PathToIntervals] = None) = {
    this(bams           = in.map(b => b -> out.resolve(PathUtil.basename(b) + ".cleaned" + PathUtil.extensionOf(b).getOrElse(""))).toMap,
         ref            = ref,
         known          = known,
         intervals      = intervals,
         bamCompression = None)
    Files.createDirectories(out)
  }

  /** Constructs a mini pipeline that runs RealignerTargetCreator and IndelRealigner on multiple samples together. */
  override def build(): Unit = {
    val ioMap: FilePath = Files.createTempFile("indel_cleaning.", ".map")
    val indelCleaningIntervals = Files.createTempFile("indel_cleaning.", ".intervals")
    val targetCreator = new RealignerTargetCreator(in = bams.keys.toSeq, out = indelCleaningIntervals, known = known, ref = ref, intervals = intervals)
    val makeIoMap = SimpleInJvmTask("MakeIndelRealignerMap", {
      val lines = bams.map(pair => pair._1.getFileName.toString + "\t" + pair._2.toString).toSeq
      Io.writeLines(ioMap, lines)
    })
    val indelRealigner = new IndelRealigner(in = bams.keys.toSeq, inOutMap = ioMap, ref = ref, known = known, targetIntervals = indelCleaningIntervals, bamCompression = bamCompression)
    val cleanup = new DeleteFiles(ioMap, indelCleaningIntervals).withName("DeleteIndelTmpFiles")

    root ==> (targetCreator :: makeIoMap) ==> indelRealigner ==> cleanup
  }
}

/** Runs the realigner target creator step of the process. */
class RealignerTargetCreator(val in: Seq[PathToBam],
                             val out: FilePath,
                             val known: Seq[PathToVcf],
                             ref: PathToFasta,
                             intervals: Option[PathToIntervals])
  extends GatkTask(walker = "RealignerTargetCreator", ref = ref, intervals = intervals) {

  require(gatkMajorVersion < 4, s"RealignerTargetCreator is not supported in GATK v$gatkMajorVersion.")
  override protected def addWalkerArgs(buffer: ListBuffer[Any]): Unit = {
    in.foreach(bam => buffer.append("-I", bam))
    buffer.append("-o", out)
    known.foreach(vcf => buffer.append("-known", vcf))
  }
}

/** Runs the BAM transforming IndelRealigner step. */
class IndelRealigner(val in: Seq[PathToBam],
                     val inOutMap: FilePath,
                     ref: PathToFasta,
                     val known: Seq[PathToVcf],
                     val targetIntervals: FilePath,
                     bamCompression: Option[Int] = None)
  extends GatkTask(walker = "IndelRealigner", ref = ref, bamCompression = bamCompression) {

  require(gatkMajorVersion < 4, s"IndelRealigner is not supported in GATK v$gatkMajorVersion.")

  override protected def addWalkerArgs(buffer: ListBuffer[Any]): Unit = {
    in.foreach(bam => buffer.append("-I", bam))
    known.foreach(vcf => buffer.append("-known", vcf))
    buffer.append("-nWayOut", inOutMap)
    buffer.append("-targetIntervals", targetIntervals)
  }
}
