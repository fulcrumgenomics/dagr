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

import scala.collection.Traversable
import scala.util.{Failure, Success, Try}

/** Very simple command line parser.
  *
  * 1. Option specifications should be specified using methods in [[OptionLookup]]: [[OptionLookup.acceptFlag()]],
  * [[OptionLookup.acceptSingleValue()]], [[OptionLookup.acceptMultipleValues()]].
  * 2. Call [[parse()]] to parse the argument strings.
  * 3. Either (1) Query for option values in [[OptionLookup]]: [[OptionLookup.hasOptionValues]] and
  * [[OptionLookup.optionValues()]], or (2) traverse tuples of name and values using [[OptionParser().foreach()]] or
  * similar methods.
  *
  * See the README.md for more information on valid arguments to [[OptionParser]]. */
class OptionParser(val argFilePrefix: Option[String] = None) extends OptionLookup with Traversable[(OptionName, List[OptionValue])] {
  private var remainingArgs: Traversable[String] = Nil

  /** returns any remaining args that were not parsed in the previous call to `parse`. */
  def remaining: Traversable[String] = remainingArgs

  /** Parse the given args. If an error was found, the first error is returned */
  def parse(args: String*): Try[this.type] = parse(args.toList)

  /** Parse the given args. If an error was found, the first error is returned */
  def parse(args: List[String]) : Try[this.type] = {
    val argTokenizer = new ArgTokenizer(args, argFilePrefix=argFilePrefix)
    val argTokenCollator = new ArgTokenCollator(argTokenizer)

    argTokenCollator.foreach {
      case Failure(failure) => return Failure(failure)
      case Success(ArgOptionAndValues(name, values)) =>
        addOptionValues(name, values:_*) match {
          case Failure(failure) => return Failure(failure)
          case _ => Unit
        }
    }

    remainingArgs = argTokenizer.takeRemaining
    Success(this)
  }

  /** Applies the given function to the options with values.  If an error occurred in parsing, there will be no options */
  override def foreach[U](f: ((OptionName, List[OptionValue])) => U): Unit = {
    this.optionMap
      .values.toList.distinct.filter(_.nonEmpty).foreach { optionValues =>
      f(optionValues.optionNames.head, optionValues.toList)
    }
  }
}
