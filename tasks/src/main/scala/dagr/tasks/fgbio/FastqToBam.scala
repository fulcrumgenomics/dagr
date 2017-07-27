/*
 * The MIT License
 *
 * Copyright (c) 2017 Fulcrum Genomics LLC
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

package dagr.tasks.fgbio

import com.fulcrumgenomics.commons.CommonsDef._
import dagr.core.tasksystem.PipeOut
import dagr.tasks.DataTypes.SamOrBam
import htsjdk.samtools.util.Iso8601Date

import scala.collection.mutable.ListBuffer

class FastqToBam(val input: Seq[PathToFastq],
                 val readStructures: Seq[String],
                 val sample: String,
                 val library: String,
                 val output: PathToBam,
                 val sort: Boolean,
                 val umiTag: Option[String] = None,
                 val readGroupId: Option[String] = None,
                 val platform: Option[String] = None,
                 val platformUnit: Option[String] = None,
                 val platformModel: Option[String] = None,
                 val sequencingCenter: Option[String] = None,
                 val predictedInsertSize: Option[Int] = None,
                 val description: Option[String] = None,
                 val comment: Seq[String] = Seq.empty,
                 val runDate: Option[Iso8601Date] = None
                ) extends FgBioTask with PipeOut[SamOrBam] {
  override protected def addFgBioArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("--input")
    buffer.append(input:_*)
    if (readStructures.nonEmpty) {
      buffer.append("--read-structures")
      buffer.append(readStructures:_*)
    }
    buffer.append("--sample", sample)
    buffer.append("--library", library)
    buffer.append("--output", output)
    buffer.append("--sort", sort)
    umiTag.foreach(x => buffer.append("--umi-tag", x))
    readGroupId.foreach(x => buffer.append("--read-group-id", x))
    platform.foreach(x => buffer.append("--platform", x))
    platformUnit.foreach(x => buffer.append("--platform-unit", x))
    platformModel.foreach(x => buffer.append("--platform-model", x))
    sequencingCenter.foreach(x => buffer.append("--sequencing-center", x))
    predictedInsertSize.foreach(x => buffer.append("--predicted-insert-size", x))
    description.foreach(x => buffer.append("--description", x))
    runDate.foreach(x => buffer.append("--run-date", x))
    if (comment.nonEmpty) {
      buffer.append("--comment")
      buffer.append(comment:_*)
    }
  }
}
