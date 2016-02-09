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

import java.text.{DecimalFormat, NumberFormat}

object StringUtil {

  /** Wraps a given string with a prefix and optional suffix */
  def wrapString(prefix: String, str: String, suffix: String = ""): String = s"$prefix$str$suffix"

  /**
    * Return input string with newlines inserted to ensure that all lines
    * have length <= maxLineLength.  If a word is too long, it is simply broken
    * at maxLineLength.  Does not handle tabs intelligently (due to implementer laziness).
    */
  def wordWrap(s: String, maxLength: Int): String = {
    s.split(" ").foldLeft(Array(""))( (out, in) => {
      if ((out.last + " " + in).trim.length > maxLength) out :+ in
      else out.updated(out.length - 1, out.last + " " + in)
    }).mkString("n").trim
  }

  /** Computes the levenshtein distance between two strings
    *
    * @param string1 the first string
    * @param string2 the second string
    * @param swap the swap penalty
    * @param substitution the substitution penalty
    * @param insertion the insertion penalty
    * @param deletion the deletion penalty
    * @return the levenshtein distance
    */
  def levenshteinDistance(string1: String, string2: String, swap: Int, substitution: Int, insertion: Int, deletion: Int): Int = {
    val str1 = string1.getBytes()
    val str2 = string2.getBytes()

    var row0: Array[Int] = Array.fill[Int](string2.length+1)(0)
    var row1 = Array.fill[Int](string2.length+1)(0)
    var row2: Array[Int] = Array.fill[Int](string2.length+1)(0)

    row1 = row1.zipWithIndex.map{ case (v: Int, i: Int) => i * insertion}

    str1.zipWithIndex.foreach { case (s: Byte, i: Int) =>

      row2(0) = (i + 1) * deletion

      str2.zipWithIndex.foreach { case (t: Byte, j: Int) =>

        row2(j + 1) = row1(j)
        if (str1(i) != str2(j)) {
          row2(j + 1) += substitution
        }

        if (i > 0 && j > 0 && str1(i - 1) == str2(j) && str1(i) == str2(j - 1) && row2(j + 1) > row0(j - 1) + swap) {
          row2(j + 1) = row0(j - 1) + swap
        }

        if (row2(j + 1) > row1(j + 1) + deletion) {
          row2(j + 1) = row1(j + 1) + deletion
        }

        if (row2(j + 1) > row2(j) + insertion) {
          row2(j + 1) = row2(j) + insertion
        }
      }

      val dummy = row0
      row0 = row1
      row1 = row2
      row2 = dummy
    }

    row1(str2.size)
  }

  /**
    * Takes in a camel-case string and converts it to a GNU option style string by identifying capital letters and
    * replacing them with a hyphen followed by the lower case letter.
    */
  def camelToGnu(in:String) : String = {
    val builder = new StringBuilder
    val chs = in.toCharArray
    for (i <- 0 until chs.length) {
      val ch = chs(i)

      if (ch.isUpper) {
        if (i > 0) builder.append("-")
        builder.append(ch.toLower)
      }
      else {
        builder.append(ch)
      }
    }

    builder.toString()
  }

  /**
    * Formats a number of seconds into days:hours:minutes:seconds.
    */
  def formatElapsedTime(seconds: Long): String = {
    val timeFmt: NumberFormat = new DecimalFormat("00")
    val s: Long = seconds % 60
    val allMinutes: Long = seconds / 60
    val m: Long = allMinutes % 60
    val allHours: Long = allMinutes / 60
    val h: Long = allHours % 24
    val d: Long = allHours / 24
    List(d, h, m, s).map(timeFmt.format).mkString(":")
  }

  /** A simple version of Unix's `column` utility.  This assumes the table is NxM. */
  def columnIt(rows: List[List[String]], delimiter: String = " "): String = {
    try {
      // get the number of columns
      val numColumns = rows.head.size
      // for each column, find the maximum length of a cell
      val maxColumnLengths = 0.to(numColumns - 1).map { i => rows.map(_ (i).length).max }
      // pad each row in the table
      rows.map { row =>
        0.to(numColumns - 1).map { i =>
          val cell = row(i)
          val formatString = "%" + maxColumnLengths(i) + "s"
          String.format(formatString, cell)
        }.mkString(delimiter)
      }.mkString("\n")
    }
    catch {
      case ex: java.lang.IndexOutOfBoundsException =>
        throw new IllegalArgumentException("columnIt failed.  Did you forget to input an NxM table?")
    }
  }
}
