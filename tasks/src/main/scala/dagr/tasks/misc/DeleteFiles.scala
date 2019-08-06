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
package dagr.tasks.misc

import java.nio.file.{Path, Files}
import java.util.stream.Collectors

import com.fulcrumgenomics.commons.CommonsDef._
import dagr.core.tasksystem.SimpleInJvmTask

/**
  * Simple tasks that deletes one or more extent files. If a path represents a directory
  * then the directory and all of it's children will be deleted.
  */
class DeleteFiles(val paths: FilePath*) extends SimpleInJvmTask {
  withName("Delete_" + paths.size + "_Files")
  requires(cores=0.1, memory="4m")

  override def run(): Unit = paths.foreach(delete)

  /** Recursive implementation that recurses if path is a directory, then deletes the path. */
  private def delete(path: Path): Unit = {
    if (Files.isDirectory(path)) {
      val childStream = Files.list(path)
      val children = childStream.collect(Collectors.toList())
      childStream.close()
      children.iterator.foreach(this.delete)
    }

    Files.deleteIfExists(path)
  }
}
