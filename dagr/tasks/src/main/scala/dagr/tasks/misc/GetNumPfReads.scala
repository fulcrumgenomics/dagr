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
package dagr.tasks.misc

import java.nio.file.Path

import dagr.core.tasksystem.{SimpleInJvmTask, InJvmTask}
import htsjdk.samtools.metrics.MetricsFile
import picard.analysis.AlignmentSummaryMetrics

import scala.collection.JavaConversions._

class GetNumPfReads(alignmentSummaryMetrics: Path) extends SimpleInJvmTask {
  var numPfReads: Long = 0

  /**
    * Abstract method to be implemented by subclasses to perform any tasks. Should throw an exception
    * to indicate an error or problem during processing.
    */
  override def run(): Unit = {
    val metricsList: java.util.List[AlignmentSummaryMetrics] = MetricsFile.readBeans(alignmentSummaryMetrics.toFile).asInstanceOf[java.util.List[AlignmentSummaryMetrics]]
    val metrics: List[AlignmentSummaryMetrics] = metricsList.toList
    numPfReads = metrics.filter(m => m.CATEGORY == AlignmentSummaryMetrics.Category.PAIR).head.PF_READS
  }
}
