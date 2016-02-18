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

package dagr.sopt.cmdline

/** Base class for all exceptions thrown by the command line parsing */
class CommandLineException(msg: String, e: Option[Exception] = None) extends RuntimeException(msg, e.orNull) {
  def this(msg: String, e: Exception) = this(msg, Some(e))
}

/** Exception thrown when the user forgets to specify an argument */
case class MissingArgumentException(msg: String, e: Option[Exception] = None) extends CommandLineException(msg = msg, e = e) {
  def this(msg: String, e: Exception) = this(msg, Some(e))
}

/** Exception thrown when the user forgets to specify an annotation */
case class MissingAnnotationException(msg: String, e: Option[Exception] = None) extends CommandLineException(msg = msg, e = e) {
  def this(msg: String, e: Exception) = this(msg, Some(e))
}

/** Exception thrown when trying to convert to specific value */
case class ValueConversionException(msg: String, e: Option[Exception] = None) extends CommandLineException(msg = msg, e = e) {
  def this(msg: String, e: Exception) = this(msg, Some(e))
}

/** Exception thrown when something internally goes wrong with command line parsing */
case class CommandLineParserInternalException(msg: String, e: Option[Exception] = None) extends CommandLineException(msg = msg, e = e) {
  def this(msg: String, e: Exception) = this(msg, Some(e))
}

/** Exception thrown when something there is user error (never happens) */
case class UserException(msg: String, e: Option[Exception] = None) extends CommandLineException(msg = msg, e = e) {
  def this(msg: String, e: Exception) = this(msg, Some(e))
}

/** Exception thrown when something the user gives a bad value */
case class BadArgumentValue(msg: String, e: Option[Exception] = None) extends CommandLineException(msg = msg, e = e) {
  def this(msg: String, e: Exception) = this(msg, Some(e))
}

/** Exception thrown when something the annotation on a field is incorrect. */
case class BadAnnotationException(msg: String, e: Option[Exception] = None) extends CommandLineException(msg = msg, e = e) {
  def this(msg: String, e: Exception) = this(msg, Some(e))
}
