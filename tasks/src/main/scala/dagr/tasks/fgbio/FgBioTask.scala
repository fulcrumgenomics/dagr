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

import java.nio.file.Path

import com.fulcrumgenomics.commons.util.LogLevel
import dagr.core.config.Configuration
import dagr.core.exec.{Cores, Memory}
import dagr.core.tasksystem.{FixedResources, ProcessTask}
import dagr.tasks.DagrDef.DirPath
import dagr.tasks.JarTask

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object FgBioTask {
  val FgBioJarConfigPath = "fgbio.jar"
}

/**
  * Base Task for any task in the FgBio jar.
  *
  * @param compressionLevel the compress level to use for HTSJDK.
  */
abstract class FgBioTask(var compressionLevel: Option[Int] = None,
                         val asyncIo: Option[Boolean] = None,
                         val tmpDir: Option[DirPath] = None,
                         val logLevel: Option[LogLevel] = None)
  extends ProcessTask with JarTask with FixedResources with Configuration {
  requires(Cores(1), Memory("4G"))

  /** Looks up the first super class that does not have "\$anon\$" in its name. */
  lazy val commandName: String = JarTask.findCommandName(getClass, Some("FgBioTask"))

  name = commandName

  override final def args: Seq[Any] = {
    val buffer = ListBuffer[Any]()
    val jvmProps = mutable.Map[String,String]()
    buffer.appendAll(jarArgs(this.fgBioJar, jvmProperties=jvmProps, jvmMemory=this.resources.memory))
    asyncIo.foreach(a => buffer.append("--async-io", a))
    compressionLevel.foreach(c => buffer.append("--compression", c))
    tmpDir.foreach(t => buffer.append("--tmp-dir", t))
    logLevel.foreach(l => buffer.append("--log-level", l))
    buffer += commandName
    addFgBioArgs(buffer)
    buffer
  }

  /** Can be overridden to use a specific FgBio jar. */
  protected def fgBioJar: Path = configure[Path](FgBioTask.FgBioJarConfigPath)

  /** Implement this to add the tool-specific arguments */
  protected def addFgBioArgs(buffer: ListBuffer[Any]): Unit

  /** Sets the compression level to a specific value. */
  def withCompression(i: Int) : this.type = { this.compressionLevel = Some(i); this }
}
