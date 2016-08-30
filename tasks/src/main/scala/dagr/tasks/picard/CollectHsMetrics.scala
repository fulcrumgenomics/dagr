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

import dagr.commons.io.PathUtil
import dagr.tasks.DagrDef
import DagrDef.{PathToBam, PathToFasta, PathToIntervals}

import scala.collection.mutable.ListBuffer

object CollectHsMetrics {
  val MetricsExtension   = ".hybrid_selection_metrics"
  val PerTargetExtension = ".per_target_coverage"
  def baitSetName(baitSetIntervals: Path): String = PathUtil.basename(baitSetIntervals.toString, trimExt=true)
}

class CollectHsMetrics(override val in: PathToBam,
                       override val prefix: Option[Path] = None,
                       ref: PathToFasta,
                       targets: PathToIntervals,
                       baits: Option[PathToIntervals] = None,
                       baitSetName: Option[String] = None,
                       minimumBaseQuality: Option[Int] = None)
  extends PicardTask with PicardMetricsTask {

  override def metricsExtension: String = CollectHsMetrics.MetricsExtension

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("I=" + in)
    buffer.append("O=" + metricsFile)
    buffer.append("R=" + ref)
    buffer.append("TI=" + targets)
    buffer.append("BI=" + baits.getOrElse(targets))
    buffer.append("BAIT_SET_NAME=" + baitSetName.getOrElse(CollectHsMetrics.baitSetName(targets)))
    buffer.append("LEVEL=ALL_READS")
    buffer.append("PER_TARGET_COVERAGE=" + metricsFile(extension=CollectHsMetrics.PerTargetExtension, kind=PicardOutput.Text))
    minimumBaseQuality.foreach { q => buffer.append("MINIMUM_BASE_QUALITY=" + q) }
  }
}
