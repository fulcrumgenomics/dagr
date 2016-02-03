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
package dagr.core.util

import java.nio.file.{Path, Paths}

object PathUtil {

  val illegalCharacters: String = "[!\"#$%&'()*/:;<=>?@\\^`{|}~] "

  def sanitizeFileName(fileName: String, illegalCharacters: String = PathUtil.illegalCharacters, replacement: Option[Char] = Some('_')): String = {
    if (replacement.isEmpty) fileName.filter(c => !illegalCharacters.contains(c))
    else fileName.map(c => if (illegalCharacters.contains(c)) replacement.get else c)
  }

  /** Replaces the extension on an existing path. */
  def replaceExtension(path: Path, ext:String) : Path = {
    val name = path.getFileName.toString
    val index = name.lastIndexOf('.')
    val newName = (if (index > 0) name.substring(0, index) else name) + ext
    path.resolveSibling(newName)
  }

  /** Remove the extension from a filename if present (the last . to the end of the string). */
  def removeExtension(path: Path) : Path = replaceExtension(path, "")

  /** Remove the extension from a filename if present (the last . to the end of the string). */
  def removeExtension(pathname: String) : String = {
    removeExtension(Paths.get(pathname)).toString
  }

  /** Works similarly to the unix command basename, by optionally removing an extension, and all leading path elements. */
  def basename(name: Path, trimExt: Boolean = true) : String = {
    val x = if (trimExt) removeExtension(name) else name
    x.getFileName.toString
  }

  /** Works similarly to the unix commmand basename, by optionally removing an extension, and all leading path elements. */
  def basename(name: String, trimExt: Boolean) : String = {
   basename(Paths.get(name), trimExt)
  }
}
