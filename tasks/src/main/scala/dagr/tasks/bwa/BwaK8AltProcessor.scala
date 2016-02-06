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
package dagr.tasks.bwa

import java.nio.file.Path

import dagr.core.config.Configuration
import dagr.tasks.DataTypes.Sam
import dagr.core.tasksystem.{FixedResources, Pipe, ProcessTask}

object BwaK8AltProcessor {
  val BwaKitDirConfigKey = "bwa-kit.dir"
  val ScriptName = "bwa-postalt.js"
}

/**
 * Task for running the alt-mapping post-processing setup from bwa.kit. By default
 * reads from stdin, but an input file can be supplied.  Writes to stdout.
 */
class BwaK8AltProcessor(in: Option[Path] = None, altFile: Path) extends ProcessTask
  with FixedResources with Configuration with Pipe[Sam,Sam] {

  override def args = {
    val bwaKit = configure[Path](BwaK8AltProcessor.BwaKitDirConfigKey)
    bwaKit.resolve("k8") :: bwaKit.resolve(BwaK8AltProcessor.ScriptName) :: altFile :: in.toList
  }
}
