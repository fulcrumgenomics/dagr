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
import dagr.core.tasksystem.Pipe
import dagr.tasks.DataTypes.SamOrBam
import dagr.tasks.DagrDef
import DagrDef.PathToBam

import scala.collection.mutable.ListBuffer

object MarkIlluminaAdapters {
  val MetricsExtension = ".adapter_metrics"
}

class MarkIlluminaAdapters(override val in: PathToBam,
                           out: PathToBam,
                           override val prefix: Option[Path],
                           fivePrimeAdapter: Option[String] = None,
                           threePrimeAdapter: Option[String] = None)
  extends PicardTask with PicardMetricsTask with Pipe[SamOrBam,SamOrBam] {
  requires(Cores(1), Memory("1G"))

  override def metricsExtension: String = MarkIlluminaAdapters.MetricsExtension

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("I=" + in)
    buffer.append("O=" + out)
    buffer.append("M=" + metricsFile)
    fivePrimeAdapter.foreach(v  => buffer.append("FIVE_PRIME_ADAPTER=" + v))
    threePrimeAdapter.foreach(v => buffer.append("THREE_PRIME_ADAPTER=" + v))
  }
}
