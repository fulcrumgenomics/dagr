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

import java.nio.file.Path

import dagr.tasks.{PathToBam, PathToFasta}

import scala.collection.mutable.ListBuffer

object CollectAlignmentSummaryMetrics {
   def getMetricsExtension: String = ".alignment_summary_metrics"
}

class CollectAlignmentSummaryMetrics(in: PathToBam,
                                     prefix: Option[Path],
                                     ref: PathToFasta,
                                     assumeSorted: Boolean = true,
                                     programs: List[MetricsProgram.Value] = MetricsProgram.values.toList)
  extends PicardMetricsTask(input = in, prefix = prefix) {

  override def getMetricsExtension: String = CollectAlignmentSummaryMetrics.getMetricsExtension

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("I=" + in)
    buffer.append("O=" + getMetricsFile)
    buffer.append("R=" + ref)
    buffer.append("AS=" + assumeSorted)
    programs.foreach(program => buffer.append("PROGRAM=" + program.toString))
  }
}
