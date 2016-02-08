package dagr.tasks.bwa

import java.nio.file.Files

import dagr.core.config.Configuration
import dagr.core.execsystem.{Cores, Memory}
import dagr.tasks.DataTypes.{Fastq, Sam, SamOrBam}
import dagr.core.tasksystem.{Pipes, Pipe}
import dagr.core.util.{Io, PathUtil}
import dagr.tasks._
import dagr.tasks.picard.{FifoBuffer, MergeBamAlignment, SamToFastq}
import htsjdk.samtools.SAMFileHeader.SortOrder

/**
  * Constants and defaults that are used across types of bwa invocation
  */
object Bwa extends Configuration {
  val BwaExecutableConfigKey: String = "bwa.executable"

  def findBwa = configureExecutable(BwaExecutableConfigKey, "bwa")

  /**
    * Constructs a piped task that pipes from an unmapped SAM or BAM in queryname order through
    * bwa-mem, optionally through bwa.kit's alt contig re-mapping process, and then through
    * [[MergeBamAlignment]] to create a well-formed aligned BAM.
    *
    * @param unmappedBam the BAM in which to read unmapped data.
    * @param mappedBam the BAM in which to store mapped data
    * @param ref the reference FASTA file, with BWA indexes pre-built
    * @param sortOrder the desired sort order of the output BAM file
    * @param minThreads the minimum number of threads to allocate to bwa-mem
    * @param maxThreads the maximum number of threads to allocate to bwa-mem
    * @param bwaMemMemory the amount of memory based on the referenceFasta
    * @param samToFastqMem the amount of memory for SamToFastq
    * @param mergeBamAlignmentMem the amount of memory for MergeBamAlignment
    * @param fifoBufferMem the amount of memory for each FifoBuffer
    */
  def bwaMemStreamed(unmappedBam: PathToBam,
                     mappedBam: PathToBam,
                     ref: PathToFasta,
                     sortOrder: SortOrder = SortOrder.coordinate,
                     minThreads: Int = 1,
                     maxThreads: Int = 32,
                     bwaMemMemory: String = "8G",
                     samToFastqMem: String = "512M",
                     mergeBamAlignmentMem: String = "2G",
                     fifoBufferMem: String = "512M",
                     processAltMappings : Boolean = true
                    ): Pipe[SamOrBam,SamOrBam] = {
    val alt = PathUtil.pathTo(ref + ".alt")
    val doAlt = Files.exists(alt) && processAltMappings
    val fifoMem: Memory = Memory(fifoBufferMem)

    val samToFastq = SamToFastq(in=unmappedBam, out=Io.StdOut).requires(Cores(0.5), Memory(samToFastqMem))
    val fqBuffer   = new FifoBuffer[Fastq].requires(memory=fifoMem)
    val bwaMem     = new BwaMem(fastq=Io.StdIn, ref=ref, minThreads=minThreads, maxThreads=maxThreads, memory=Memory(bwaMemMemory))
    val bwaBuffer  = new FifoBuffer[Sam].requires(memory=fifoMem)
    val altPipe    =  if (doAlt) new BwaK8AltProcessor(altFile=alt) | new FifoBuffer[Sam].requires(memory=fifoMem) else Pipes.empty[Sam]
    val mergeBam   = new MergeBamAlignment(unmapped=unmappedBam, mapped=Io.StdIn, out=mappedBam, ref=ref, sortOrder=sortOrder).requires(memory=Memory(mergeBamAlignmentMem))
    (samToFastq | fqBuffer | bwaMem | bwaBuffer | altPipe | mergeBam).withName("BwaMemThroughMergeBam")
  }
}
