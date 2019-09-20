/*
 * The MIT License
 *
 * Copyright (c) 2019 Fulcrum Genomics LLC
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
import dagr.tasks.DagrDef.{FilePath, PathToBam}
import dagr.tasks.DataTypes.SamOrBam

import scala.collection.mutable.ListBuffer

case class CorrectUmis(in: PathToBam,
                       out: PathToBam,
                       maxMismatches: Int,
                       minDistance: Int,
                       rejects: Option[PathToBam] = None,
                       metrics: Option[FilePath] = None,
                       umis: Seq[String] = Seq.empty,
                       umiFiles: Seq[FilePath] = Seq.empty,
                       umiTag: Option[String] = None,
                       dontStoreOriginalUmis: Boolean = false
                      ) extends FgBioTask with Pipe[SamOrBam, SamOrBam]{

  override protected def addFgBioArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("-i", in)
    buffer.append("-o", out)
    buffer.append("-m", maxMismatches)
    buffer.append("-d", minDistance)
    rejects.foreach {r => buffer.append("-r", r) }
    metrics.foreach {m => buffer.append("-M", m) }
    if (umis.nonEmpty)     { buffer.append("-u"); buffer.append(umis: _*)     }
    if (umiFiles.nonEmpty) { buffer.append("-U"); buffer.append(umiFiles: _*) }
    umiTag.foreach { t => buffer.append("-t", t) }
    buffer.append("-x", dontStoreOriginalUmis)
  }
}
