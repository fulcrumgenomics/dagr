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

import dagr.core.tasksystem.{Pipe, PipeIn}
import dagr.tasks.DagrDef.{PathPrefix, PathToBam, PathToIntervals}
import dagr.tasks.DataTypes.SamOrBam

import scala.collection.mutable.ListBuffer

class CollectDuplexSeqMetrics(input: PathToBam,
                              output: PathPrefix,
                              intervals: Option[PathToIntervals] = None,
                              description: Option[String] = None,
                              minAbReads: Option[Int] = None,
                              minBaReads: Option[Int] = None
                             ) extends FgBioTask with PipeIn[SamOrBam] {

  override protected def addFgBioArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("-i", input)
    buffer.append("-o", output)
    intervals.foreach  (x => buffer.append("-l", x))
    description.foreach(x => buffer.append("-d", x))
    minAbReads.foreach (x => buffer.append("-a", x))
    minBaReads.foreach (x => buffer.append("-b", x))
  }
}
