package dagr.tasks.bwa

import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.{FixedResources, Pipeline, ProcessTask, VariableResources}
import dagr.commons.io.{Io, PathUtil}
import dagr.tasks.DagrDef
import DagrDef._
import dagr.tasks.misc.DeleteFiles
import dagr.tasks.picard.{MergeBamAlignment, SamToFastq}

import scala.collection.mutable.ListBuffer

/**
  * Pipeline that starts with an unmapped BAM and runs through conversion to fastq,
  * running bwa aln, bwa sampe and then MergeBamAlignment to generate a tidy mapped
  * BAM file via bwa backtrack/bwa short alignment.
  */
class BwaBacktrack(val unmappedBam : PathToBam, val mappedBam : PathToBam,
                   val ref : PathToFasta, val tmpDir: Option[DirPath] = None,
                   val maxDiffOrMissingProb: Option[Either[Int, Float]] = None,
                   val seedLength: Option[Int] = None,
                   val pairedEnd: Boolean = true
                ) extends Pipeline {
  /**
    * Strings together invocations of bwa aln and bwa sampe to generate an output SAM file.
    */
  override def build(): Unit = if (pairedEnd) buildPaired() else buildFragment()

  private def buildPaired(): Unit = {
    // Make all the paths we're going to use
    val workdir = tmpDir.getOrElse(mappedBam.getParent)
    val basename = PathUtil.basename(mappedBam) + "." + System.currentTimeMillis()
    val r1Fastq = workdir.resolve(basename + ".r1.fq.gz")
    val r2Fastq = workdir.resolve(basename + ".r2.fq.gz")
    val r1Sai   = workdir.resolve(basename + ".r1.sai")
    val r2Sai   = workdir.resolve(basename + ".r2.sai")
    val sam     = workdir.resolve(basename + ".sam")

    val samToFastq = SamToFastq(in=unmappedBam, r1Fastq=r1Fastq, r2Fastq=r2Fastq)
    val r1Aln = new BwaAln(in=r1Fastq, out=r1Sai, ref=ref, maxDiffOrMissingProb=maxDiffOrMissingProb, seedLength=seedLength)
    val r2Aln = new BwaAln(in=r2Fastq, out=r2Sai, ref=ref, maxDiffOrMissingProb=maxDiffOrMissingProb, seedLength=seedLength)
    val sampe = new BwaSampe(r1Fastq=r1Fastq, r2Fastq=r2Fastq, r1Sai=r1Sai, r2Sai=r2Sai, out=sam, ref=ref)
    val rmIntermediates = new DeleteFiles(r1Fastq, r2Fastq, r1Sai, r2Sai)
    val mergeBam = new MergeBamAlignment(unmapped=unmappedBam, mapped=sam, out=mappedBam, ref=ref)
    val rmSam    = new DeleteFiles(sam)

    root ==> samToFastq ==> (r1Aln :: r2Aln) ==> sampe ==> (rmIntermediates :: mergeBam) ==> rmSam
  }

  private def buildFragment(): Unit = {
    // Make all the paths we're going to use
    val workdir = tmpDir.getOrElse(mappedBam.getParent)
    val basename = PathUtil.basename(mappedBam) + "." + System.currentTimeMillis()
    val r1Fastq = workdir.resolve(basename + ".r1.fq.gz")
    val r1Sai   = workdir.resolve(basename + ".r1.sai")
    val sam     = workdir.resolve(basename + ".sam")

    val samToFastq = new SamToFastq(in=unmappedBam, fastq1=r1Fastq)
    val r1Aln = new BwaAln(in=r1Fastq, out=r1Sai, ref=ref, maxDiffOrMissingProb=maxDiffOrMissingProb, seedLength=seedLength)
    val samse = new BwaSamse(r1Fastq=r1Fastq, r1Sai=r1Sai, out=sam, ref=ref)
    val rmIntermediates = new DeleteFiles(r1Fastq, r1Sai)
    val mergeBam = new MergeBamAlignment(unmapped=unmappedBam, mapped=sam, out=mappedBam, ref=ref)
    val rmSam    = new DeleteFiles(sam)

    root ==> samToFastq ==> r1Aln ==> samse ==> (rmIntermediates :: mergeBam) ==> rmSam
  }
}

/** Class for running "bwa aln" */
class BwaAln(val in: PathToFastq, val out: FilePath, val ref: PathToFasta, val maxCores:Int = 8,
             val maxDiffOrMissingProb: Option[Either[Int, Float]] = None,
             val seedLength: Option[Int] = None)
  extends ProcessTask with VariableResources {
  override def pickResources(resources: ResourceSet): Option[ResourceSet] = {
    resources.subset(minCores=Cores(1), maxCores=Cores(maxCores), memory = Memory("6g"))
  }

  override def args: Seq[Any] = {
    val args = ListBuffer[Any]()
    args.append(Bwa.findBwa, "aln", "-t", resources.cores.toInt)
    maxDiffOrMissingProb.foreach {
      case Left(maxDiff: Int) => args.append("-n", maxDiff)
      case Right(missingProb: Float) => args.append("-n", missingProb)
    }
    seedLength.foreach(args.append("-l", _))
    if (Io.StdOut  != out) args.append("-f", out.toString)
    args.append(ref, in)
    args
  }
}

/** Class for running "bwa sampe" */
class BwaSampe(val r1Fastq: PathToFastq, val r2Fastq: PathToFastq,
               val r1Sai:   FilePath,    val r2Sai:   FilePath,
               val out: PathToBam, val ref: PathToFasta) extends ProcessTask with FixedResources {
  // Always run with 1 core and 6g of memory :(
  requires(new ResourceSet(Cores(1), Memory("6g")))

  override def args: Seq[Any] = {
    val args = ListBuffer[Any]()
    args.append(Bwa.findBwa.toString, "sampe", "-P")
    if (Io.StdOut  != out) args.append("-f", out)
    args.append(ref, r1Sai, r2Sai, r1Fastq, r2Fastq)
    args
  }
}

/** Class for running "bwa samse" */
class BwaSamse(val r1Fastq: PathToFastq,
               val r1Sai:   FilePath,
               val out: PathToBam,
               val ref: PathToFasta) extends ProcessTask with FixedResources {
  // Always run with 1 core and 6g of memory :(
  requires(new ResourceSet(Cores(1), Memory("6g")))

  override def args: Seq[Any] = {
    val args = ListBuffer[Any]()
    args.append(Bwa.findBwa.toString, "samse")
    if (Io.StdOut  != out) args.append("-f", out)
    args.append(ref, r1Sai, r1Fastq)
    args
  }
}
