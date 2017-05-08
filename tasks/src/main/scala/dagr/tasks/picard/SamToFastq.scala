/*
 * The MIT License
 *
 * Copyright (c) 2015 Fulcrum Genomics LLC
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
package dagr.tasks.picard

import com.fulcrumgenomics.commons.CommonsDef.unreachable
import dagr.core.execsystem.{Cores, Memory}
import dagr.core.tasksystem.Pipe
import dagr.tasks.DagrDef.{PathToBam, PathToFastq}
import dagr.tasks.DataTypes.{Fastq, SamOrBam}

import scala.collection.mutable.ListBuffer

object SamToFastq {
  /** Generates a SamToFastq that will generate interleaved output for pairs, or a single fastq for unpaired data. */
  def apply(in: PathToBam, out: PathToFastq): SamToFastq = new SamToFastq(in=in, fastq1=out, interleave=true)

  /** Generates a SamToFastq that will generate a separate fastq file for read1 and read2. */
  def apply(in: PathToBam, r1Fastq: PathToFastq, r2Fastq: PathToFastq): SamToFastq =
    new SamToFastq(in=in, fastq1=r1Fastq, fastq2=Some(r2Fastq), interleave=false)
}

/** Runs Picard's SamToFastq to generate a single fastq or a pair of fastq files from a BAM file. */
class SamToFastq(in: PathToBam,
                 fastq1: PathToFastq,
                 fastq2: Option[PathToFastq] = None,
                 interleave : Boolean = true,
                 clippingAttribute: Option[String] = None,
                 clippingAction: Option[String] = None,
                 clippingMinReadLength: Int = 25) extends PicardTask with Pipe[SamOrBam,Fastq] {

  requires(Cores(1), Memory("512M"))
  assert(clippingAttribute.isDefined == clippingAction.isDefined, "Must define both clipping attr and action, or neither.")

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("INPUT=" + in)
    buffer.append("F=" + fastq1)
    fastq2.foreach(p => buffer.append("F2=" + p))
    buffer.append("INTERLEAVE=" + interleave)
    buffer.append("INCLUDE_NON_PF_READS=true")

    (clippingAttribute, clippingAction) match {
      case (None, None) => /* do nothing */
      case (Some(attr), Some(action)) =>
        buffer.append("CLIPPING_ATTRIBUTE=" + attr)
        buffer.append("CLIPPING_ACTION=" + action)
        buffer.append("CLIPPING_MIN_LENGTH=" + clippingMinReadLength)
      case _ => unreachable("Clipping attribute and action must be both defined or undefined.")
    }
  }
}
