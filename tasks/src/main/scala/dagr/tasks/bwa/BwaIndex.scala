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

package dagr.tasks.bwa

import dagr.api.models.util.{Cores, Memory}
import dagr.core.tasksystem.{FixedResources, ProcessTask}
import dagr.tasks.DagrDef.PathToFasta

/**
 * Indexes a genome for use with BWA.
 */
class BwaIndex(val ref: PathToFasta) extends ProcessTask with FixedResources {
  {
    // Set a sensible default amount of resources required - can still be overridden with requires() later
    val refSize = ref.toFile.length()
    if (refSize < 2L*1024L*1024L*1024L) {
      requires(Cores(1), Memory((refSize*5.5).toLong))
    }
    else {
      requires(Cores(1), Memory(refSize*3))
    }
  }

  override def args: Seq[Any] = Bwa.findBwa :: "index" :: ref :: Nil
}
