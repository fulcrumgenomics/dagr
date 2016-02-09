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
import dagr.core.util.Io
import dagr.tasks.PathToBam

import scala.collection.mutable.ListBuffer

object MarkDuplicates {
  val MetricsExtension = ".duplicate_metrics"
}

class MarkDuplicates(in: PathToBam,
                     out: Option[PathToBam] = None,
                     prefix: Option[Path] = None,
                     comment: Option[String] = None,
                     opticalDuplicatesPixelDistance: Option[Int] = None,
                     assumeSorted: Boolean = true)
  extends PicardMetricsTask(in = out.getOrElse(in), prefix = prefix) {
  requires(Cores(1), Memory("6G"))

  override def getMetricsExtension: String = MarkDuplicates.MetricsExtension

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("I=" + in)
    buffer.append("O=" + out.getOrElse(Io.DevNull))
    buffer.append("M=" + getMetricsFile)
    buffer.append("AS=" + assumeSorted)
  }
}
