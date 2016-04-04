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

import dagr.core.execsystem.{Cores, Memory}
import dagr.commons.io.Io
import dagr.tasks.DagrDef
import DagrDef.PathToBam

import scala.collection.mutable.ListBuffer

object MarkDuplicates {
  val MetricsExtension = ".duplicate_metrics"
}

class MarkDuplicates(override val in: PathToBam,
                     out: Option[PathToBam] = None,
                     override val prefix: Option[Path] = None,
                     comment: Option[String] = None,
                     opticalDuplicatesPixelDistance: Option[Int] = None,
                     assumeSorted: Boolean = true,
                     templateUmiTag: Option[String] = None,
                     read1UmiTag: Option[String] = None,
                     read2UmiTag: Option[String] = None)
  extends PicardTask with PicardMetricsTask {
  requires(Cores(1), Memory("6G"))

  override def metricsExtension: String = MarkDuplicates.MetricsExtension

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("I=" + in)
    buffer.append("O=" + out.getOrElse(Io.DevNull))
    buffer.append("M=" + metricsFile)
    buffer.append("AS=" + assumeSorted)
    comment.foreach(v => buffer.append("COMMENT=" + v))
    opticalDuplicatesPixelDistance.foreach(v => buffer.append("OPTICAL_DUPLICATE_PIXEL_DISTANCE=" + v))
    templateUmiTag.foreach(v => buffer.append("BARCODE_TAG=" + v))
    read1UmiTag.foreach(v => buffer.append("READ_ONE_BARCODE_TAG=" + v))
    read2UmiTag.foreach(v => buffer.append("READ_TWO_BARCODE_TAG=" + v))
  }
}
