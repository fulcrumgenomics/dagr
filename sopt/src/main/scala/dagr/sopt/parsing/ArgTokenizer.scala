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

package dagr.sopt.parsing

import java.nio.file.{Files, Paths}
import java.util.NoSuchElementException

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object ArgTokenizer {
  /** The token returned by the tokenizer */
  sealed abstract class Token
  /** An option-name-only token */
  case class ArgOption(name: String) extends Token
  /** A value-only token */
  case class ArgValue(value: String) extends Token
  /** An option-name-and-value token */
  case class ArgOptionAndValue(name: String, value: String) extends Token
}

/** A class to tokenize a sequence of strings prior to option parsing.  A series of tokens will
  * be available.  If any errors are encountered, a failure will be present instead of the token.  */
class ArgTokenizer(args: Seq[String],
                   val argFilePrefix: Option[String] = None) extends Iterator[Try[ArgTokenizer.Token]] {
  import ArgTokenizer._

  private var nextToken: Option[Try[Token]] = None
  private val stack = mutable.Stack[String](args:_*)

  /** Alternate constructor that supports var-arg syntax for testing. */
  private[sopt] def this(args: String*) = this(args.toSeq, None)

  /** True if there are more tokens, false otherwise */
  override def hasNext(): Boolean = {
    if (nextToken.isEmpty) updateNextToken()
    nextToken.isDefined
  }

  /** Returns the next token, or a failure if one was encountered. */
  override def next(): Try[Token] = {
    // bad user
    if (!hasNext()) throw new NoSuchElementException("Called 'next' when 'hasNext' is false")
    val tryVal = nextToken match {
      case None        => throw new NoSuchElementException("Called 'next' when 'hasNext' is false")
      case Some(value) => value
    }
    nextToken = None
    tryVal
  }

  /** Returns any remaining args that were not tokenized. This is a destructive operation, so should only be called once. */
  def takeRemaining: Seq[String] = this.stack.toSeq

  /** Sets the `nextToken` if it is not defined and we have more strings in the iterator */
  private def updateNextToken(): Unit = if (nextToken.isEmpty && this.stack.nonEmpty) nextToken = takeNextToken

  /**
    * Requires that the stack have at least one item left in it; pulls the top item from the
    * stack and processes it.
    */
  private def takeNextToken: Option[Try[Token]] = (this.stack.pop(), argFilePrefix) match {
    case ("--", _) => None
    case ("",   _) => Some(Failure(new OptionNameException("Empty argument given.")))
    case (arg,  _) if arg.startsWith("--") => Some(convertDoubleDashOption(arg.substring(2)))
    case (arg,  _) if arg.startsWith("-") => Some(convertSingleDash(arg.substring(1)))
    case (arg, Some(pre)) if arg.startsWith(pre) =>
      loadArgumentFile(arg.drop(pre.length)) match {
        case Failure(failure) => Some(Failure(failure))
        case Success(newArgs) =>
          this.stack.pushAll(newArgs.reverseIterator)
          if (this.stack.nonEmpty) takeNextToken else None
      }
    case (arg, _) => Some(Success(ArgValue(value = arg)))
  }

  /** Loads an arguments file and returns the list of tokens it contains. */
  private def loadArgumentFile(filename: String): Try[Seq[String]] = {
    Try { Files.readAllLines(Paths.get(filename)).map(s => s.trim).filter(s => s.nonEmpty).toSeq }
  }

  /** If the arg was an option (leading dash or dashes) but has no characters after the dash, create an appropriate
    * exception wrapped in a failure. */
  private def emptyFailure(arg: String): Try[Token] = {
    Failure(OptionNameException(s"Option names must have at least one character after the leading dash; found: '$arg'"))
  }

  /** Given an short name option string without the leading dash, returns the option name, and optionally
    * a value for that option if one exists.  The latter may happen if the short name and value are concatenated or
    * separated by an '=' character.
    */
  private def convertSingleDash(input: String): Try[Token] = {
    if (input.isEmpty) emptyFailure(input)
    else {
      val name = input.substring(0, 1)
      input.substring(1) match {
        case "" => Success(ArgOption(name = name))
        // NB: must have characters after the "="
        case value if value.startsWith("=") && value.length > 1 => Success(ArgOptionAndValue(name = name, value = value.substring(1)))
        case value => Success(ArgOptionAndValue(name = name, value = value))
      }
    }
  }

  /** Given an long name option string without the leading dashes, and returns the option name, and optionally
    * a value for that option if one exists.  The latter may happen if the long name and value are separated by an '='
    * character.  If no value is give after the "=", the a failure is returned.
    */
  private def convertDoubleDashOption(input: String): Try[Token] = {
    val idx = input.indexOf('=')
    (input.take(idx), input.drop(idx+1)) match {
      case (before, after) if before.isEmpty => Success(ArgOption(name = after))
      case (before, after) if after.isEmpty => Failure(new OptionNameException(s"Trailing '=' found in option '$before'; did you forget a value?"))
      case (before, after) => Success(ArgOptionAndValue(name = before, value = after))
    }
  }
}
