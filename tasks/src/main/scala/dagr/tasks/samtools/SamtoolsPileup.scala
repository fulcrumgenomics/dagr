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
package dagr.tasks.samtools

import java.nio.file.Path

import dagr.core.tasksystem.FixedResources
import dagr.tasks.{PathToBam, PathToFasta, PathToIntervals}

import scala.collection.mutable.ListBuffer

/**
  * Runs samtools pileup.
  */
class SamtoolsPileup(ref: PathToFasta,
                     regions: Option[PathToIntervals] = None,
                     bam: PathToBam,
                     output: Option[Path],
                     maxDepth: Int = 5000,
                     minMappingQuality: Int = 1,
                     minBaseQuality: Int = 13)
  extends SamtoolsTask("mpileup") with FixedResources {

  override def addSubcommandArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("--fasta-ref", ref.toString)
    regions.foreach(r => buffer.append("--positions", r.toString))
    output.foreach(f => buffer.append("--output", f.toString))
    buffer.append("--max-depth", maxDepth.toString)
    buffer.append("--min-MQ", minMappingQuality.toString)
    buffer.append("--min-BQ", minBaseQuality.toString)
    buffer.append(bam)
  }
}
