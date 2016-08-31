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

package dagr.tasks.fgbio

import dagr.commons.CommonsDef._
import dagr.core.execsystem.{Cores, Memory}
import dagr.core.tasksystem.Pipe
import dagr.tasks.DataTypes.SamOrBam

import scala.collection.mutable.ListBuffer

class ExtractUmisFromBam(val in: PathToBam,
                         val out: PathToBam,
                         val readStructures: Seq[String],
                         val umiTags: Seq[String],
                         val annotateNames: Boolean = true,
                         val clippingAttribute: Option[String] = None
                        ) extends FgBioTask with Pipe[SamOrBam,SamOrBam] {
  requires(Cores(1), Memory("1G"))

  protected def addFgBioArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("-i", in)
    buffer.append("-o", out)
    if (readStructures.nonEmpty) {
      buffer.append("-r")
      buffer.append(readStructures: _*)
    }
    if (umiTags.nonEmpty) {
      buffer.append("-b")
      buffer.append(umiTags: _*)
    }
    buffer.append("-a", annotateNames)
    clippingAttribute.foreach { tag => buffer.append("-c", tag) }
  }
}