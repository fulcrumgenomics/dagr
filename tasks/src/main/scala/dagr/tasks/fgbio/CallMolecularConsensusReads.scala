/*
 * The MIT License
 *
 * Copyright (c) 2016 Fulcrum Genomics LLC
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

package dagr.tasks.fgbio

import dagr.core.tasksystem.Pipe
import dagr.tasks.DagrDef.PathToBam
import dagr.tasks.DataTypes.SamOrBam

import scala.collection.mutable.ListBuffer

/**
 * Task to run CallMolecularConsensusReads on a BAM file that has been run through
 * GroupReadsByUmi already.  Produces an unmapped BAM file!
 */
class CallMolecularConsensusReads(val in: PathToBam,
                                  val out: PathToBam,
                                  val minReads: Int,
                                  val rejects: Option[PathToBam] = None,
                                  val tag: Option[String] = None,
                                  val readNamePrefix: Option[String] = None,
                                  val readGroupId: Option[String] = None,
                                  val errorRatePreUmi: Option[Int] = None,
                                  val errorRatePostUmi: Option[Int] = None,
                                  val minInputBaseQuality: Option[Int] = None
                                 ) extends FgBioTask with Pipe[SamOrBam, SamOrBam] {

  override protected def addFgBioArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("-i", in)
    buffer.append("-o", out)
    buffer.append("-M", minReads)
    rejects.foreach                      (x => buffer.append("-r", x))
    tag.foreach                          (x => buffer.append("-t", x))
    readNamePrefix.foreach               (x => buffer.append("-p", x))
    readGroupId.foreach                  (x => buffer.append("-R", x))
    errorRatePreUmi.foreach              (x => buffer.append("-1", x))
    errorRatePostUmi.foreach             (x => buffer.append("-2", x))
    minInputBaseQuality.foreach          (x => buffer.append("-m", x))
  }
}
