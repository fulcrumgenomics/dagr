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
package dagr.tasks.picard

import dagr.core.execsystem.{Cores, Memory}
import dagr.core.tasksystem.Pipe
import dagr.tasks.DataTypes.SamOrBam
import dagr.tasks.{PathToBam, PathToFasta}
import htsjdk.samtools.SAMFileHeader.SortOrder

import scala.collection.mutable.ListBuffer

class MergeBamAlignment( unmapped: PathToBam,
                         mapped: PathToBam,
                         out: PathToBam,
                         ref: PathToFasta,
                         programGroupId: Option[String] = None,
                         programGroupVersion: Option[String] = None,
                         programGroupCommand: Option[String] = None,
                         programGroupName: Option[String] = None,
                         attributesToRetain: List[String] = List[String]("X0", "ZS", "ZI", "ZM", "ZC", "ZN"),
                         orientation: String = "FR",
                         maxGaps: Int = -1,
                         sortOrder: SortOrder = SortOrder.coordinate)
  extends PicardTask with Pipe[SamOrBam,SamOrBam] {
  requires(Cores(1), Memory("4g"))

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("UNMAPPED=" + unmapped)
    buffer.append("ALIGNED=" + mapped)
    buffer.append("O=" + out)
    buffer.append("R=" + ref)
    buffer.append("CLIP_ADAPTERS=false")
    programGroupId.foreach(pg => buffer.append("PG=" + pg))
    programGroupVersion.foreach(version => buffer.append("PG_VERSION=" + version))
    programGroupCommand.foreach(cmd => buffer.append("PG_COMMAND=\'" + cmd + "\'"))
    programGroupName.foreach(name => buffer.append("PG_NAME=" + name))
    attributesToRetain.foreach(attr => buffer.append("ATTRIBUTES_TO_RETAIN=" + attr))
    buffer.append("ORIENTATIONS=" + orientation)
    buffer.append("MAX_GAPS=" + maxGaps)
    buffer.append("SO=" + sortOrder.name())
  }
}
