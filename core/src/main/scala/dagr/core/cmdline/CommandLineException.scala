/*
 * The MIT License
 *
 * Copyright (c) 2015-6 Fulcrum Genomics LLC
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
package dagr.core.cmdline

private[core] object CommandLineException {
  private[core] def format(msg: String, e: Exception): String = {
    msg + ": " + e.getMessage + "\n" + e.getStackTrace.mkString("\n")
  }
}

/** Base class for all exceptions thrown by the command line parsing */
private[core] class CommandLineException(msg: String) extends RuntimeException(msg) {
  def this(msg: String, e: Exception) = {
    this(CommandLineException.format(msg, e))
  }
}

/** Exception thrown when the user forgets to specify an argument */
private[core] case class MissingArgumentException(msg: String) extends CommandLineException(msg = msg) {
  def this(msg: String, e: Exception) = {
    this(CommandLineException.format(msg, e))
  }
}

/** Exception thrown when the user forgets to specify an annotation */
private[core] case class MissingAnnotationException(msg: String) extends CommandLineException(msg = msg) {
  def this(msg: String, e: Exception) = {
    this(CommandLineException.format(msg, e))
  }
}

/** Exception thrown when trying to convert to specific value */
private[core] case class ValueConversionException(msg: String) extends CommandLineException(msg = msg) {
  def this(msg: String, e: Exception) = {
    this(CommandLineException.format(msg, e))
  }
}

/** Exception thrown when something internally goes wrong with command line parsing */
private[core] case class CommandLineParserInternalException(msg: String) extends CommandLineException(msg = msg) {
  def this(msg: String, e: Exception) = {
    this(CommandLineException.format(msg, e))
  }
}

/** Exception thrown when something there is user error (never happens) */
private[core] case class UserException(msg: String) extends CommandLineException(msg = msg) {
  def this(msg: String, e: Exception) = {
    this(CommandLineException.format(msg, e))
  }
}

/** Exception thrown when something the user gives a bad value */
private[core] case class BadArgumentValue(msg: String) extends CommandLineException(msg = msg) {
  def this(msg: String, e: Exception) = {
    this(CommandLineException.format(msg, e))
  }
}

/** Exception thrown when something the annotation on a field is incorrect. */
private[core] case class BadAnnotationException(msg: String) extends CommandLineException(msg = msg) {
  def this(msg: String, e: Exception) = {
    this(CommandLineException.format(msg, e))
  }
}
