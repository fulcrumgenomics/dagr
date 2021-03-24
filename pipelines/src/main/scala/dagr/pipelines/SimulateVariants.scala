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
import dagr.core.execsystem.{Cores, Memory}
import dagr.core.tasksystem._
import dagr.tasks.DagrDef.{PathToIntervals, _}
import dagr.tasks.bwa.BwaMem
import dagr.tasks.gatk._
import dagr.tasks.misc.DWGSim
import dagr.tasks.picard.SortSam
import htsjdk.samtools.SAMFileHeader.SortOrder

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

  var bams: Seq[PathToBam] = Nil

  override def build(): Unit = {

    Files.createDirectories(out)
    val coverage = depth

    bams = vcfs.map(vcf => {

      val toTable = new VariantsToTable(in = vcf, out = out.resolve(prefix + vcf.getFileName + ".table"), fields = "CHROM" :: "POS" :: "HET" :: "HOM-VAR" :: Nil, intervals = Some(targets))
      val makeBed = new MakePLBed(table = toTable.out, out = out.resolve(prefix + "PL.bed"))

      val rando = new CopyPS_FromBedToVcf(in = vcf, out = out.resolve(prefix + vcf.getFileName + ".with.pl.vcf"),
        dpHeader = dpHeader, pathToBed = makeBed.out)

      val subsetToPL = new subsetToPL(in = rando.out, out = out.resolve(prefix + vcf.getFileName + ".subsetToPL.vcf"))
      val normalize = new LeftAlignAndTrimVariants(in = subsetToPL.out, out = out.resolve(prefix + vcf.getFileName + ".normalized.vcf"), ref = ref, splitMultiAlleic = Some(true))
      val index = new IndexVariants(in = normalize.out)


      root ==> toTable ==> makeBed ==> rando ==> subsetToPL ==> normalize ==> index

      val simulate:Seq[DWGSim] = Range(0, 22).map {chr => {
        val subsetBed = new Grep(in = targets, what = s"'^chr${chr + 1}\t'", out = out.resolve(prefix + PathUtil.removeExtension(targets.getFileName) + s".chr${chr + 1}.bed"))
        root ==> subsetBed

        val sim = new DWGSim(vcf = normalize.out,
          fasta = ref,
          outPrefix = out.resolve(prefix + vcf.getFileName + s".chr${chr + 1}.sim"),
          depth = coverage,
          coverageTarget = subsetBed.out)

        (index::subsetBed) ==> sim

        sim
      }}

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

    quoteIfNecessary = false

    override def args: Seq[Any] = grep ::
      what ::
      in.toAbsolutePath ::
      ">" ::
      out.toAbsolutePath ::
      "||" ::
      "touch" ::
      out.toAbsolutePath ::
      Nil
  }

  // copies the value from the bed file to the vcf
  private class Concatenate(val ins: Seq[Path],
                            val out: Path
                           ) extends ProcessTask with FixedResources with Configuration {
    requires(Cores(1), Memory("1G"))

    quoteIfNecessary = false

    private val cat: Path = configureExecutable("cat.executable", "cat")

    override def args: Seq[Any] = cat ::
      ins.map{in => in.toAbsolutePath} ::
      ">" ::
      out.toAbsolutePath ::
      Nil
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
