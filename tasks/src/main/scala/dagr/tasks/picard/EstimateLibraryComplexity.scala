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

import dagr.tasks.PathToBam

import scala.collection.mutable.ListBuffer

object EstimateLibraryComplexity {
  def getMinIdenticalBases(minIdenticalBases: Int, numPfReads: Option[Long]): Int = {
    if (numPfReads.isEmpty || numPfReads.get < 10000000) {
      minIdenticalBases
    }
    else {
      math.ceil( (math.log(numPfReads.get) / math.log(4)) - 7 ).toInt
    }
  }

  val MetricsExtension = ".els_duplicate_metrics"
}

class EstimateLibraryComplexity(in: PathToBam,
                                prefix: Option[Path],
                                var minIdenticalBases: Int = 5,
                                var numPfReads: Option[Long] = None)
  extends PicardMetricsTask(in = in, prefix = prefix) {

  override def getMetricsExtension: String = EstimateLibraryComplexity.MetricsExtension

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("I=" + in)
    buffer.append("O=" + getMetricsFile)
    buffer.append("MIN_IDENTICAL_BASES=" + EstimateLibraryComplexity.getMinIdenticalBases(minIdenticalBases = minIdenticalBases,
                                                                                          numPfReads = numPfReads))
  }
}
