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

import dagr.tasks.PathToIntervals
import picard.util.IntervalListTools.Action

import scala.collection.mutable.ListBuffer

/**
  * Task to run Picard's IntervalListTools.
  */
class IntervalListTools(val in:  Seq[PathToIntervals],
                        val secondIn: Seq[PathToIntervals] = Nil,
                        val out: PathToIntervals,
                        val padding: Int = 0,
                        val sort: Boolean = true,
                        val unique: Boolean = false,
                        val action: Action = Action.CONCAT,
                        val invert: Boolean = false
                       ) extends PicardTask {
  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    in.foreach(p => buffer += "I=" + p)
    secondIn.foreach(p => buffer += "SI=" + p)
    buffer += "O=" + out
    buffer += "PADDING=" + padding
    buffer += "SORT=" + sort
    buffer += "UNIQUE=" + unique
    buffer += "ACTION=" + action.name()
    buffer += "INVERT=" + invert
  }
}
