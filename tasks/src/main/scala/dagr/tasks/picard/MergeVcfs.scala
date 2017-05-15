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

import com.fulcrumgenomics.commons.io.Io
import dagr.core.tasksystem.PipeOut
import dagr.tasks.DagrDef.PathToVcf
import dagr.tasks.DataTypes.Vcf

import scala.collection.mutable.ListBuffer

/**
 * Runs Picard's MergeVcfs to pull together multiple VCFs with identical sample lists
 */
class MergeVcfs(val in : Seq[PathToVcf], val out: PathToVcf = Io.StdOut) extends PicardTask with PipeOut[Vcf] {

  if (out == Io.StdOut) this.createIndex = Some(false)

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    in.foreach { v => buffer += "I=" + v }
    buffer += "O=" + out
  }
}
