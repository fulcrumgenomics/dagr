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

import dagr.tasks.DagrDef
import DagrDef.PathToBam
import htsjdk.samtools.SAMFileHeader.SortOrder

import scala.collection.mutable.ListBuffer

/**
 * Task for running MergeSamFiles with one or more inputs.
 */
class MergeSamFiles(in: Traversable[PathToBam], out: PathToBam, sortOrder: SortOrder) extends PicardTask(createIndex = Some(sortOrder == SortOrder.coordinate)) {
  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    in.foreach(p => buffer.append("INPUT=" + p))
    buffer.append("OUTPUT=" + out)
    buffer.append("SO=" + sortOrder.name())
  }
}
