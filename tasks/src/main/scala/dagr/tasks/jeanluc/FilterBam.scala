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
package dagr.tasks.jeanluc

import dagr.tasks.DagrDef
import DagrDef.{PathToBam, PathToIntervals}

import scala.collection.mutable.ListBuffer

/**
  * Task for filtering a BAM to a specific set of intervals while removing unmapped
  * and duplicate reads.
  */
class FilterBam(val in: PathToBam,
                val out: PathToBam,
                val intervals: Option[PathToIntervals]) extends JeanLucTask {

  override def addJeanLucArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("INPUT=" + in)
    buffer.append("OUTPUT=" + out)
    intervals.foreach(i => buffer.append("INTERVALS=" + i))
  }
}
