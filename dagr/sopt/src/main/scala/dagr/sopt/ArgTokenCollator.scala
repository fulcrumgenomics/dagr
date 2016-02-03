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

import dagr.sopt.ArgTokenizer.{ArgOptionAndValue, ArgValue, Token, ArgOption}
import dagr.sopt.util.PeekableIterator

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
  private val iterator = new PeekableIterator[Try[Token]](argTokenizer)
  private var nextOption: Option[Try[ArgOptionAndValues]] = None

  this.advance() // to initialize nextOption

  def hasNext: Boolean = nextOption.isDefined

  def next: Try[ArgOptionAndValues] = {
    val retVal = nextOption.get
    this.advance()
    retVal
  }

  private def advance(): Unit = {
    if (!iterator.hasNext) {
      this.nextOption = None
      return
    }

    val values: ListBuffer[String] = new ListBuffer[String]()

    // try to get an a token that has an option name
    val nameTry = iterator.next match {
      case Success(ArgOption(name)) => Success(name)
      case Success(ArgOptionAndValue(name, value)) => values += value; Success(name)
      case Success(ArgValue(value)) => Failure(new OptionNameException(s"Illegal option: '$value'"))
      case Failure(ex) => Failure(ex)
      case Success(tok) => throw new IllegalStateException(s"Unknown token: '${tok.getClass.getSimpleName}")
    }
    nameTry match {
      case Failure(ex) => this.nextOption = Some(Failure(ex))
      case Success(name) =>
        // find values with the same option name, may only be ArgValue and ArgOptionAndValue
        while (iterator.hasNext && ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue(iterator.peek, name)) {
          iterator.next match {
            case Success(ArgValue(value)) => values += value
            case Success(ArgOptionAndValue(`name`, value)) => values += value
            case _ => throw new IllegalStateException("Should never reach here")
          }
        }
        this.nextOption = Some(Success(new ArgOptionAndValues(name = name, values = values)))
    }
  }
}

