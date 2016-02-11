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

package dagr.sopt

import dagr.sopt.ArgTokenizer.{ArgOption, ArgOptionAndValue, ArgValue, Token}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

case class ArgOptionAndValues(name: String, values: Seq[String])

object ArgTokenCollator {
  private[sopt] def isArgValueOrSameNameArgOptionAndValue(tryToken: Try[Token], name: String): Boolean = {
    tryToken match {
      case Success(ArgValue(value)) => true
      case Success(ArgOptionAndValue(`name`, value)) => true
      case _ => false
    }
  }
}

/** Collates Tokens into name and values, such that there is no value without an associated option name. */
class ArgTokenCollator(argTokenizer: ArgTokenizer) extends Iterator[Try[ArgOptionAndValues]] {
  private val iterator = argTokenizer.buffered
  private var nextOption: Option[Try[ArgOptionAndValues]] = None

  this.advance() // to initialize nextOption

  /** True if there is another value, false otherwise.  */
  def hasNext: Boolean = nextOption.isDefined

    /** Gets the next token wrapped in a Try.  A failure is returned if the provided [[ArgTokenizer]] returned a failure
      * or if we could not find an option name ([[ArgOption]] or [[ArgOptionAndValue]] before finding an
      * option value ([[ArgValue]]).
      */
  def next: Try[ArgOptionAndValues] = {
    val retVal = nextOption match {
      case None => throw new NoSuchElementException("'next' was called when 'hasNext' is false")
      case Some(value) => value
    }
    this.advance()
    retVal
  }

  /** Tries to get the next token that has an option name, and adds any values to `values` if found. */
  private def nextName(values: ListBuffer[String]): Try[String] = {
    iterator.next match {
      case Success(ArgOption(name)) => Success(name)
      case Success(ArgOptionAndValue(name, value)) => values += value; Success(name)
      case Success(ArgValue(value)) => Failure(new OptionNameException(s"Illegal option: '$value'"))
      case Failure(ex) => Failure(ex)
    }
  }

  /** Find values with the same option name, may only be ArgValue and ArgOptionAndValue. */
  private def addValuesWithSameName(name: String, values: ListBuffer[String]): Unit = {
    // find values with the same option name, may only be ArgValue and ArgOptionAndValue
    while (iterator.hasNext && ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue(iterator.head, name)) {
      iterator.next match {
        case Success(ArgValue(value)) => values += value
        case Success(ArgOptionAndValue(`name`, value)) => values += value
        case _ => throw new IllegalStateException("Should never reach here")
      }
    }
  }

  /** Advance the underlying iterator and update `nextOption`. */
  private def advance(): Unit = {
    if (!iterator.hasNext) {
      this.nextOption = None
    }
    else {
      val values: ListBuffer[String] = new ListBuffer[String]()

      // First try to get an a token that has an option name, next add any subsequent tokens that have just values, or
      // the same option name with values.
      nextName(values) match {
        case Failure(ex) => this.nextOption = Some(Failure(ex))
        case Success(name) =>
          addValuesWithSameName(name, values)
          // gather them all back up
          this.nextOption = Some(Success(new ArgOptionAndValues(name = name, values = values)))
      }
    }
  }
}

