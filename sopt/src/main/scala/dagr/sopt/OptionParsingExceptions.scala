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

/** Exception classes for various option parsing errors */
sealed abstract class OptionParsingException(message: String) extends RuntimeException(message)

/** For errors in specifying the option name, for example missing leading dashes, nothing after the leading dashes, ... */
case class OptionNameException(message: String) extends OptionParsingException(message)

/** For errors when specifying too few values for an option */
case class TooFewValuesException(message: String) extends OptionParsingException(message)

/** For errors when specifying too many values for an option */
case class TooManyValuesException(message: String) extends OptionParsingException(message)

/** For errors when specifying an option too many times */
case class OptionSpecifiedMultipleTimesException(message: String) extends OptionParsingException(message)

/** For errors when parsing the value for a flag field */
case class IllegalFlagValueException(message: String) extends OptionParsingException(message)

/** For errors when the same option name is given for different options */
case class DuplicateOptionNameException(message: String) extends OptionParsingException(message)

/** For errors when the same option name is not found */
case class IllegalOptionNameException(message: String) extends OptionParsingException(message)
