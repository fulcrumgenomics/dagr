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
package dagr.tasks.gatk

import java.nio.file.Path

import dagr.core.config.Configuration
import dagr.core.execsystem.{Cores, Memory}
import dagr.core.tasksystem.{FixedResources, ProcessTask}
import dagr.tasks.DagrDef.{PathToFasta, PathToIntervals}
import dagr.tasks.{DagrDef, JarTask}

import scala.collection.mutable.ListBuffer

object Gatk4Task {
  val GatkJarPathConfigKey = "gatk4.jar"
}

/**
  * Abstract base class for tasks that involve running the GATK.
  */


abstract class Gatk4Task(val walker: String,
                         val ref: Option[PathToFasta] = None,
                         val intervals: Option[PathToIntervals] = None,
                         val bamCompression: Option[Int] = None)
  extends ProcessTask with JarTask with FixedResources with Configuration {
  requires(Cores(1), Memory("4g"))

  override def args: Seq[Any] = {
    val buffer = ListBuffer[Any]()
    buffer.appendAll(jarArgs(this.gatkJar, jvmMemory = this.resources.memory))
    buffer.addOne(this.walker)
    ref.foreach { r => buffer.addOne("-R").addOne(r.toAbsolutePath.toString) }
    intervals.foreach(il => buffer.addOne("-L").addOne(il.toAbsolutePath.toString))
    bamCompression.foreach(c => buffer.addOne("--bam_compression").addOne(c))
    addWalkerArgs(buffer)

    buffer.toSeq
  }

  /** Can be overridden to use a specific GATK jar. */
  protected def gatkJar: Path = configure[Path](Gatk4Task.GatkJarPathConfigKey)

  protected def addWalkerArgs(buffer: ListBuffer[Any]): Unit
}
