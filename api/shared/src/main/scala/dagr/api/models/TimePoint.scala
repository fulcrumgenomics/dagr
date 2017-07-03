/*
 * The MIT License
 *
 * Copyright (c) 2017 Fulcrum Genomics
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
 *
 */

package dagr.api.models

import java.time.Instant

object TimePoint {
  /*
  def parse(s: String, f: Int => TaskStatus): TimePoint = {
    s.split(",", 1).toList match {
      case instant :: status =>
        val status = TaskStatus.parse(status)
        TimePoint(
          status = f(status.ordinal),
          instant = instant
        )
      case _ =>
        throw new IllegalArgumentException(s"Could not parse TimePoint '$s'")
    }
  }
  */

  /*
  def parse(s: String): TimePoint = {
    s.split(",", 1).toList match {
      case instant :: status :: Nil =>
        TimePoint(
          status  = TaskStatus.parse(status),
          instant = Instant.parse(instant) // FIXME: does not work in scala-js
        )
      case _ =>
        throw new IllegalArgumentException(s"Could not parse TimePoint '$s'")
    }
  }
  */
}

/** A tuple representing the instant the task was set to the given status. */
case class TimePoint(status: TaskStatus, instant: Instant) {
  /*
  override def toString: String = {
    s"${this.instant},${this.status.name},${this.status.ordinal},${this.status.success},${this.status.description}"
  }
  */
}