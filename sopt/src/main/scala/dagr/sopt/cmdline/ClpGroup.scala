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

/**
  * Trait for groups of CommandLinePrograms.
  */
trait ClpGroup extends Ordered[ClpGroup] {
  /** Gets the name of this program. **/
  def name : String
  /** Gets the description of this program. **/
  def description : String
  /** The rank of the program group relative to other program groups. */
  def rank : Int = 1024
  /** Order groups by rank, then by name. */
  override def compare(that: ClpGroup): Int = {
    if (this.rank != that.rank) this.rank.compareTo(that.rank)
    else this.name.compareTo(that.name)
  }
}

/** The program group for the command line programs. */
class Clps extends ClpGroup {
  val name: String = "Clps"
  val description: String = "Various command line programs."
}
