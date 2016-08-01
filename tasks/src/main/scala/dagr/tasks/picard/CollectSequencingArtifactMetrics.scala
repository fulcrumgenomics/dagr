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

package dagr.tasks.picard

import dagr.tasks.DagrDef._

import scala.collection.mutable.ListBuffer

class CollectSequencingArtifactMetrics(val in: PathToBam,
                                       override val prefix: Option[PathPrefix] = None,
                                       val ref: PathToFasta,
                                       val intervals: Option[PathToIntervals] = None,
                                       val variants: Option[PathToVcf] = None,
                                       val minBq: Option[Int] = None,
                                       val minMq: Option[Int] = None,
                                       val includeUnpaired: Option[Boolean] = None,
                                       val contextSize: Option[Int] = None,
                                       val extension: Option[String] = Some(".txt'")
                                      ) extends PicardTask with PicardMetricsTask {

  // No single extension since multiple outputs
  override def metricsExtension: String = ""

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer += s"I=$in"
    buffer += s"O=$pathPrefix"
    buffer += s"R=$ref"
    intervals.foreach      (i => buffer += s"INTERVALS=$i")
    variants.foreach       (v => buffer += s"DB_SNP=$v")
    minBq.foreach          (q => buffer += s"Q=$q")
    minMq.foreach          (q => buffer += s"MQ=$q")
    includeUnpaired.foreach(i => buffer += s"INCLUDE_UNPAIRED=$i")
    contextSize.foreach    (s => buffer += s"CONTEXT_SIZE=$s")
    extension.foreach      (e => buffer += s"EXT=$e")
  }
}
