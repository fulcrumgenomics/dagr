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

import java.nio.file.{Path, Paths}

import dagr.tasks.{PathToBam, PathToFasta, PathToIntervals}

import scala.collection.mutable.ListBuffer

object CollectTargetedPcrMetrics {
  def getPerTargetName(metricsFile: Path): Path = {
    Paths.get(metricsFile.toString + ".per_target")
  }

  def getMetricsExtension: String = ".targeted_pcr_metrics"
}

class CollectTargetedPcrMetrics(in: PathToBam,
                                prefix: Option[Path],
                                ref: PathToFasta,
                                targets: PathToIntervals)
  extends PicardMetricsTask(input = in, prefix = prefix) {

  override def getMetricsExtension: String = CollectTargetedPcrMetrics.getMetricsExtension

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("I=" + in)
    buffer.append("O=" + getMetricsFile)
    buffer.append("R=" + ref)
    buffer.append("TARGET_INTERVALS=" + targets)
    buffer.append("AMPLICON_INTERVALS=" + targets)
    buffer.append("LEVEL=ALL_READS")
    buffer.append("PER_TARGET_COVERAGE=" + CollectTargetedPcrMetrics.getPerTargetName(getMetricsFile))
  }
}
