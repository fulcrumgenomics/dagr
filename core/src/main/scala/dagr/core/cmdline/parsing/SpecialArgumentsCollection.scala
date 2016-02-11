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
package dagr.core.cmdline.parsing

import dagr.core.cmdline.Arg

private[parsing] object SpecialArgumentsCollection {
  val HELP_FULLNAME: String = "help"
  val VERSION_FULLNAME: String = "version"
  //val ARGUMENTS_FILE_FULLNAME: String = "arguments-file"
}

/**
  * This collection is for arguments that require special treatment by the arguments parser itself.
  * It should not grow beyond a very short list.
  *
  * An arguments file will split apart by spaces, but all neighbouring strings without a leading
  * dash ('-') will be grouped.  Use multiple lines to ungroup the arguments
  *   Example 1:
  *   File: "--aab s --b -c -d wer we -c"
  *   Args: "--aab" "s" "--b" "-c" "-d" "wer we" "-c"
  *
  *   Example 2:
  *   File: "--aab s --b -c -d wer\nwe -c"
  *   Args: "--aab" "s" "--b" "-c" "-d" "wer", "we" "-c"
  */
private[parsing] final case class SpecialArgumentsCollection(
  @Arg(flag = "h", name = "help", doc = "Display the help message.", special = true)
  var help: Boolean = false,
  @Arg(name = "version", doc = "Display the version number for this tool.", special = true)
  var version: Boolean = false
)
