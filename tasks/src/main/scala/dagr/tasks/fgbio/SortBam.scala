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

import com.fulcrumgenomics.commons.io.Io
import dagr.core.tasksystem.Pipe
import dagr.tasks.DagrDef.PathToBam
import dagr.tasks.DataTypes.SamOrBam

import scala.collection.mutable.ListBuffer

class SortBam(val in: PathToBam,
              val out: PathToBam,
              val sortOrder: Option[String],
              val maxInMemory: Option[Int]
             ) extends FgBioTask with Pipe[SamOrBam,SamOrBam] {

  if (out == Io.StdOut) compressionLevel = Some(0)

  override protected def addFgBioArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("-i", in)
    buffer.append("-o", out)
    sortOrder.foreach(so => buffer.append("-s", so))
    maxInMemory.foreach(m => buffer.append("-m", m))
  }
}
