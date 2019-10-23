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

import java.nio.file.{Files, Path}

import dagr.core.tasksystem.SimpleInJvmTask
import com.fulcrumgenomics.commons.io.PathUtil

import scala.collection.mutable.ListBuffer

/**
  * Class to remove a BAM file and also the BAI file if it exists.  Succeeds if the files can be deleted,
  * *or* if no files were present to be deleted.
  */
class DeleteBam(val bam: Path*) extends SimpleInJvmTask {
  name = if (bam.length == 1) "DeleteBam." + PathUtil.basename(bam.head) else s"Delete${bam.length}Bams"
  requires(cores=0.1, memory="4m")

  override def run(): Unit = {
    val paths = ListBuffer[Path](bam:_*)
    bam.foreach { b =>
      PathUtil.extensionOf(b) match {
        case Some(".bam") =>
          paths += PathUtil.pathTo(bam.toString + ".bai")
          paths += PathUtil.pathTo(bam.toString + ".csi")
          paths += PathUtil.replaceExtension(b, ".bai")
        case Some(".cram") =>
          paths += PathUtil.pathTo(bam.toString + ".crai")
        case _ =>
      }
    }

    paths.foreach(Files.deleteIfExists)
  }
}
