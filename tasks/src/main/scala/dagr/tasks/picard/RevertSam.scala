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
package dagr.tasks.picard

import dagr.api.models.{Cores, Memory}
import dagr.tasks.DagrDef._
import htsjdk.samtools.SAMFileHeader.SortOrder

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

object RevertSam {
  lazy val DefaultAttributesToClear = new picard.sam.RevertSam().ATTRIBUTE_TO_CLEAR.toSeq
}

class RevertSam(val in: PathToBam,
                val out: PathToBam,
                sortOrder: SortOrder = SortOrder.queryname,
                restoreOriginalQualities: Boolean = true,
                removeDuplicateInformation: Boolean = true,
                removeAlignmentInformation: Boolean = true,
                attributesToClear: Seq[String] = RevertSam.DefaultAttributesToClear,
                santize: Boolean = false)
  extends PicardTask {
  requires(Cores(1), Memory("4G"))

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("I=" + in)
    buffer.append("O=" + out)
    buffer.append("SO=" + sortOrder)
    buffer.append("RESTORE_ORIGINAL_QUALITIES=" + restoreOriginalQualities)
    buffer.append("REMOVE_DUPLICATE_INFORMATION=" + removeDuplicateInformation)
    buffer.append("REMOVE_ALIGNMENT_INFORMATION=" + removeAlignmentInformation)
    buffer.append("ATTRIBUTE_TO_CLEAR=null") // always clear the attributes since we set them below
    attributesToClear.foreach(attr => buffer.append("ATTRIBUTE_TO_CLEAR=" + attr))
    buffer.append("SANITIZE=" + santize)
  }
}

