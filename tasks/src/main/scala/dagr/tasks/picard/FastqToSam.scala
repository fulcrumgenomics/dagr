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

import dagr.core.tasksystem.Pipe
import dagr.tasks.DataTypes.{Fastq, SamOrBam}
import dagr.tasks.DagrDef
import DagrDef.{PathToBam, PathToFastq}
import htsjdk.samtools.util.FastqQualityFormat

import scala.collection.mutable.ListBuffer

class FastqToSam(fastq1: PathToFastq,
                 fastq2: Option[PathToFastq] = None,
                 out: PathToBam,
                 sample: String,
                 platform: Option[String] = Some("ILLUMINA"),
                 platformUnit: Option[String] = None,
                 library: Option[String] = None,
                 readGroupName: Option[String] = None,
                 stripUnpairedMateNumber: Boolean = true,
                 useSequentialFastqs: Boolean = false,
                 qualityFormat: Option[FastqQualityFormat] = None)
  extends PicardTask with Pipe[Fastq,SamOrBam]{

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    // add custom args
    buffer.append("F1=" + fastq1)
    fastq2.foreach(f => buffer.append("F2=" + f))
    qualityFormat.foreach(f => buffer.append("QUALITY_FORMAT=" + f.name()))
    buffer.append("O=" + out)
    readGroupName.foreach(rg => buffer.append("RG=" + rg))
    buffer.append("SM=" + sample)
    platform.foreach(pl => buffer.append("PL=" + pl))
    platformUnit.foreach(pu => buffer.append("PU=" + pu))
    library.foreach(lb => buffer.append("LB=" + lb))
    if (useSequentialFastqs) buffer.append("USE_SEQUENTIAL_FASTQS=true")
    buffer.append("STRIP_UNPAIRED_MATE_NUMBER=" + stripUnpairedMateNumber)
    buffer.append("SO=queryname")
  }
}
