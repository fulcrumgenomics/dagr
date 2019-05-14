/*
 * The MIT License
 *
 * Copyright (c) 2019 Fulcrum Genomics LLC
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

import dagr.tasks.DagrDef.PathToBam
import htsjdk.samtools.SAMFileHeader.SortOrder
import htsjdk.samtools.util.Iso8601Date

import scala.collection.mutable.ListBuffer

class AddOrReplaceReadGroups(
  val in: PathToBam,
  val out: PathToBam,
  val sampleName: String,
  val library: String,
  val platform: String,
  val platformUnit: String,
  val id: Option[String] = None,
  val sortOrder: Option[SortOrder] = None,
  val sequencingCenter: Option[String] = None,
  val description: Option[String] = None,
  val runDate: Option[Iso8601Date] = None,
  val keySequence: Option[String] = None,
  val readGroupFlowOrder: Option[String] = None,
  val predictedInsertSize: Option[Int] = None,
  val programGroup: Option[String] = None,
  val platformModel: Option[String] = None
) extends PicardTask {

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("I=" + in)
    buffer.append("O=" + out)
    buffer.append("RGSM=" + sampleName)
    buffer.append("RGLB=" + library)
    buffer.append("RGPL=" + platform)  // The RGPL tag value can only be restricted set of spec-compliant values.
    buffer.append("RGPU=" + platformUnit)
    id.foreach(tag => buffer.append("RGID=" + tag))
    sortOrder.foreach(so => buffer.append("SORT_ORDER=" + so))
    sequencingCenter.foreach(tag => buffer.append("RGCN=" + tag))
    description.foreach(tag => buffer.append("RGDS=" + tag))
    runDate.foreach(tag => buffer.append("RGDT=" + tag))
    keySequence.foreach(tag => buffer.append("RGKS=" + tag))
    readGroupFlowOrder.foreach(tag => buffer.append("RGFO=" + tag))
    predictedInsertSize.foreach(tag => buffer.append("RGPI=" + tag))
    programGroup.foreach(tag => buffer.append("RGPG=" + tag))
    platformModel.foreach(tag => buffer.append("RGPM=" + tag))
  }
}
