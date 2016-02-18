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

import java.nio.file.Files

import dagr.core.execsystem.{Cores, Memory}
import dagr.tasks.DagrDef
import DagrDef.PathToBam
import dagr.tasks.picard.DownsamplingStrategy.DownsamplingStrategy

import scala.collection.mutable.ListBuffer

object DownsamplingStrategy extends Enumeration {
  type DownsamplingStrategy = Value
  val HighAccuracy, ConstantMemory, Chained = Value
}

/**
  * Constructs a new DownsampleSam task.  If 'strategy' is not supplied then a heuristic is used to determine
  * an appropriate strategy and amount of memory to be used.  If 'strategy' is supplied then the caller should
  * specify an appropriate Memory resource level.
  */
class DownsampleSam(in: PathToBam, out: PathToBam, var proportion: Double,
                    val accuracy: Option[Double] = None, strategy: Option[DownsamplingStrategy] = None) extends PicardTask {

  private val strat = {
    // Make a guess at how many SAMRecords are in the file based on a back of the napkin
    // of 40-bytes per record, or a totally arbitrary number if we're reading from a stream
    val guess = if (Files.isRegularFile(in)) Files.size(in) / 40 else 100e6.toInt
    val retentionGuess = guess * proportion

    // Pick a strategy
    if (retentionGuess > 50e6) DownsamplingStrategy.ConstantMemory
    else (strategy, accuracy) match {
      case (Some(s), _)    => s
      case (None, None)    => DownsamplingStrategy.ConstantMemory
      case (None, Some(a)) => if (guess > 2e6) DownsamplingStrategy.Chained else DownsamplingStrategy.HighAccuracy
      case _               => DownsamplingStrategy.ConstantMemory
    }
  }

  // Default the amount of memory to be used
  {
    val memory = strat match {
      case DownsamplingStrategy.ConstantMemory => "2g"
      case DownsamplingStrategy.HighAccuracy   => "4g"
      case DownsamplingStrategy.Chained        => "8g"
    }
    requires(Cores(1), Memory(memory))
  }


  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    val p: Double = math.min(1.0, proportion)
    buffer.append("I=" + in)
    buffer.append("O=" + out)
    buffer.append("P=" + p)
    buffer.append("STRATEGY=" + strat.toString)
    accuracy.foreach(acc => buffer.append("ACCURACY=" + acc))
  }
}
