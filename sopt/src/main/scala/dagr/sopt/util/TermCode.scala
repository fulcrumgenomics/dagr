/*
 * The MIT License
 *
 * Copyright (c) 2015-2016 Fulcrum Genomics LLC
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

package dagr.sopt.util

object TermCode  {
  /** True if we are to use ANSI colors to print to the terminal, false otherwise. */
  var printColor: Boolean = true
}

/** Base class for all terminal codes (ex ANSI colors for terminal output).  The apply method applies the code to the
  *  string and then terminates the code. */
abstract class TermCode {
  val code: String
  def apply(s: String): String = if (TermCode.printColor) s"$code$s${KNRM.code}" else s
}

case object KNRM extends TermCode {
  override val code: String = "\u001B[0m"
}
case object KBLD extends TermCode {
  override val code: String = "\u001B[1m"
}
case object KRED extends TermCode {
  override val code: String = "\u001B[31m"
}
case object KGRN extends TermCode {
  override val code: String = "\u001B[32m"
}
case object KYEL extends TermCode {
  override val code: String = "\u001B[33m"
}
case object KBLU extends TermCode {
  override val code: String = "\u001B[34m"
}
case object KMAG extends TermCode {
  override val code: String = "\u001B[35m"
}
case object KCYN extends TermCode {
  override val code: String = "\u001B[36m"
}
case object KWHT extends TermCode {
  override val code: String = "\u001B[37m"
}
case object KBLDRED extends TermCode {
  override val code: String = "\u001B[1m\u001B[31m"
}
case object KERROR extends TermCode {
  override val code: String = "\u001B[1m\u001B[31m"
}

