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

import dagr.core.tasksystem.Pipe
import com.fulcrumgenomics.commons.io.{Io, PathUtil}
import com.fulcrumgenomics.sopt.{arg, clp}
import dagr.core.cmdline.Pipelines
import dagr.core.config.Configuration
import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem._
import dagr.tasks.DagrDef.{PathToIntervals, _}
import dagr.tasks.DataTypes.Bed
import dagr.tasks.bwa.BwaMem
import dagr.tasks.gatk._
import dagr.tasks.misc.DWGSim
import dagr.tasks.picard.SortSam
import htsjdk.samtools.SAMFileHeader.SortOrder

import scala.collection.mutable.ListBuffer

/**
  * generate bams based on input vcfs
  */
@clp(description = "Example FASTQ to BAM pipeline.", group = classOf[Pipelines])
class SimulateVariants
(@arg(flag = 'i', doc = "Input vcfs for generating bams.") val vcfs: Seq[PathToVcf],
 @arg(flag = 'r', doc = "Reference fasta.") val ref: PathToFasta,
 @arg(flag = 'o', doc = "Output directory.") val out: DirPath,
 @arg(flag = 't', doc = "Target regions.") val targets: PathToIntervals,
 @arg(flag = 'p', doc = "Output file prefix.") val prefix: String,
 @arg(flag = 'd', doc = "Depth value to simulate.") val depth: Integer,
 @arg(flag = 'H', doc = "header line for dp.") val dpHeader: Path

) extends Pipeline(Some(out)) {

  override def build(): Unit = {

    Files.createDirectories(out)
    val coverage = depth

    val noop = new Noop()

    val beds: Seq[Path] = Range(0, 22).map { chr => {
      val subsetBed = new Grep(in = targets, what = s"^chr${chr + 1}\t", out = out.resolve(prefix + PathUtil.removeExtension(targets.getFileName) + s".chr${chr + 1}.bed"))
      subsetBed.name += s".chr${chr + 1}"

      root ==> subsetBed

      val slopBed = new BedtoolsSlop(in = subsetBed.out,
        bases = 500,
        ref = ref)

      val mergeBed = new BedtoolsMerge(out = Some(PathUtil.replaceExtension(subsetBed.out, ".slop.bed")))

      val bedSlopMerge: Pipe[Bed, Bed] = slopBed | mergeBed
      bedSlopMerge.name = s"BedSlopMerge.chr${chr + 1}"

      subsetBed ==> bedSlopMerge ==> noop

      mergeBed.out.get
    }
    }

    val bams = vcfs.map(vcf => {

      val toTable = new VariantsToTable(in = vcf, out = out.resolve(prefix + vcf.getFileName + ".table"), fields = "CHROM" :: "POS" :: "HET" :: "HOM-VAR" :: Nil, intervals = Some(targets))
      val makeBed = new MakePLBed(table = toTable.out, out = out.resolve(prefix + "PL.bed"))

      val rando = new CopyPS_FromBedToVcf(in = vcf, out = out.resolve(prefix + vcf.getFileName + ".with.pl.vcf"),
        dpHeader = dpHeader, pathToBed = makeBed.out)

      val subsetToPL = new subsetToPL(in = rando.out, out = out.resolve(prefix + vcf.getFileName + ".subsetToPL.vcf"))
      val normalize = new LeftAlignAndTrimVariants(in = subsetToPL.out, out = out.resolve(prefix + vcf.getFileName + ".normalized.vcf"), ref = ref, splitMultiAlleic = Some(true))
      val index = new IndexVariants(in = normalize.out)


      noop ==> toTable ==> makeBed ==> rando ==> subsetToPL ==> normalize ==> index

      val simulate: Seq[DWGSim] = beds.zipWithIndex.map{ case (bed, chr) => {

        val sim = new DWGSim(vcf = normalize.out,
          fasta = ref,
          outPrefix = out.resolve(prefix + vcf.getFileName + s".chr${chr + 1}.sim"),
          depth = coverage,
          coverageTarget = bed)

        sim.name += s".chr${chr + 1}"
        noop ==> sim
        index ==> sim

        sim
      }
      }

      val mergeFastas = new Concatenate(ins = simulate map { fastq => out.resolve(fastq.outputPairedFastq) },
        out = out.resolve(prefix + vcf.getFileName + ".sim.bfast.fastq"))

      val bwa = new myBwaMem(fastq = mergeFastas.out, out = Some(out.resolve(prefix + vcf.getFileName + ".tmp.sam")), ref = ref)

      simulate.map(sim => sim ==> mergeFastas)

      val sort = new SortSam(in = bwa.out.get, out = out.resolve(prefix + vcf.getFileName + ".sorted.bam"), sortOrder = SortOrder.coordinate) with Configuration {
        requires(Cores(1), Memory("2G"))
      }

      mergeFastas ==> bwa ==> sort
      sort.out
    })
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
  private class Grep(val in: Path,
                     val what: String,
                     val out: Path
                    ) extends ProcessTask with FixedResources with Configuration {
    requires(Cores(1), Memory("1G"))

    private val grep: Path = configureExecutable("grep.executable", "grep")

    override def args: Seq[Any] = grep ::
      what ::
      in.toAbsolutePath ::
      Unquotable(">") ::
      out.toAbsolutePath ::
      Unquotable("||") ::
      "touch" ::
      out.toAbsolutePath ::
      Nil
  }

  // copies the value from the bed file to the vcf
  private class Concatenate(val ins: Seq[Path],
                            val out: Path
                           ) extends ProcessTask with FixedResources with Configuration {
    requires(Cores(1), Memory("1G"))

    private val cat: Path = configureExecutable("cat.executable", "cat")

    override def args: Seq[Any] = cat ::
      ins.map { in => in.toAbsolutePath }.toList :::
      Unquotable(">") ::
      out.toAbsolutePath ::
      Nil
  }

  // adds bases to the end of bed intervals
  abstract class Bedtools(val in: Path,
                          val tool: String,
                          val out: Option[Path]
                         ) extends ProcessTask with FixedResources with Configuration with Pipe[Bed, Bed] {
    requires(Cores(1), Memory("1G"))

    private val bedtools: Path = configureExecutable("bedtools.executable", "bedtools")

    def otherArgs: List[Any] = Nil

    override def args: Seq[Any] = {
      val buffer = ListBuffer[Any](bedtools, tool, "-i", in.toAbsolutePath)

      buffer.appendAll(otherArgs)
      out.foreach(f => buffer.addOne(Unquotable(">")).addOne(f))

      buffer.toList
    }
  }

  // adds bases to the end of bed intervals
  private class BedtoolsSlop(override val in: Path,
                             val bases: Int,
                             val ref: PathToFasta,
                             override val out: Option[Path] = None
                            ) extends Bedtools(in = in, out = out, tool = "slop") {

    private val genome = PathUtil.replaceExtension(ref, ".genome")

    override def otherArgs: List[Any] =
      "-b" :: bases ::
        "-g" :: genome ::
        Nil
  }

  // merges bedfile
  private class BedtoolsMerge(override val in: Path = Io.StdIn,
                              override val out: Option[Path] = None
                             ) extends Bedtools(in = in, out = out, tool = "merge") {}

  // copies the value from the bed file to the vcf
  private class MakePLBed(val table: Path,
                          val out: Path
                         ) extends ProcessTask with FixedResources with Configuration {
    requires(Cores(1), Memory("1G"))

    private val awk: Path = configureExecutable("awk.executable", "awk")

    override def args: Seq[Any] = awk :: Unquotable(raw"""'BEGIN{OFS="\t"; } NR>1{print $$1,$$2-1, $$2, $$3+2*$$4}'""") ::
      table.toAbsolutePath :: Unquotable(">") :: out.toAbsolutePath :: Nil
  }

  private class Noop() extends ProcessTask {

    override def args: Seq[Any] = Nil

    override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = None
  }

  private class myBwaMem(val fastq: PathToFastq,
                         val out: Option[PathToBam],
                         val ref: PathToFasta) extends
    BwaMem(
      fastq = fastq,
      out = out,
      ref = ref,
      maxThreads = 2,
      memory = Memory("4G")) {
    quoteIfNecessary = false
  }

}
