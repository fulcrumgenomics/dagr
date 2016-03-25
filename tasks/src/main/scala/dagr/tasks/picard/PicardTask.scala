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

import java.nio.file.Path

import dagr.core.config.Configuration
import dagr.core.execsystem.{Cores, Memory}
import dagr.core.tasksystem.{FixedResources, ProcessTask}
import dagr.tasks.JarTask

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import htsjdk.samtools.ValidationStringency

object PicardTask {
  val PicardJarConfigPath = "picard.jar"
}

/** Simple class to run any Picard command.  Specific Picard command classes
  * should extend this.  Since the order of args is important to a task,
  * an sub-class should call getPicardArgs before adding any tasks to args, and
  * then call super.getTasks if getTasks is overriden.
  *
  * @param jvmArgs a list of arguments to the JVM.
  * @param useAdvancedGcOptions use advanced garbage collection parameters.
  * @param validationStringency set the default validation stringency for Picard.
  * @param useAsyncIo true if we are to use asynchronous IO, false otherwise.
  * @param compressionLevel the compress level to use.
  * @param createIndex true if we are to create an index, false otherwise.
  * @param createMd5File true if we are to create an Md5 file, false otherwise.
  * @param bufferSize the buffer size for the samjdk.
  */
abstract class PicardTask(var jvmArgs: List[String] = Nil,
                          var useAdvancedGcOptions: Boolean = true,
                          var validationStringency: Option[ValidationStringency] = Some(ValidationStringency.SILENT),
                          var useAsyncIo: Boolean = false,
                          var compressionLevel: Option[Int] = None,
                          var createIndex: Option[Boolean] = Some(true),
                          var createMd5File: Option[Boolean] = None,
                          var bufferSize: Option[Int] = Some(128 * 1024))
  extends ProcessTask with JarTask with FixedResources with Configuration {
  requires(Cores(1), Memory("4G"))

  /** Looks up the first super class that does not have "\$anon\$" in its name. */
  lazy val commandName: String = JarTask.findCommandName(getClass, Some("PicardTask"))

  name = commandName

  /** The path to the JAR to be executed. */
  def jarPath: Path = configure[Path](PicardTask.PicardJarConfigPath)

  override final def args: Seq[Any] = {
    val buffer = ListBuffer[Any]()

    // JVM arguments / properties
    val jvmProps = mutable.Map[String,String]()
    compressionLevel.foreach(c => jvmProps("samjdk.compression_level") = c.toString)
    bufferSize.foreach(b => jvmProps("samjdk.buffer_size") = b.toString)
    if (useAsyncIo) jvmProps("samjdk.use_async_io") = "true"
    buffer.appendAll(jarArgs(jarPath, jvmArgs=jvmArgs, jvmProperties=jvmProps,
                     jvmMemory=this.resources.memory, useAdvancedGcOptions=useAdvancedGcOptions))

    buffer += commandName
    validationStringency.foreach(v => buffer.append("VALIDATION_STRINGENCY=" + v.name()))
    createIndex.foreach(v => buffer.append("CREATE_INDEX=" + v))
    createMd5File.foreach(v =>  buffer.append("CREATE_MD5_FILE=" + v))

    addPicardArgs(buffer)
    buffer.toList
  }

  protected def addPicardArgs(buffer: ListBuffer[Any]): Unit

  // Utility methods to help set various properties

  /** Sets the compression level to a specific value. */
  def withCompression(i: Int) : this.type = { this.compressionLevel = Some(i); this }

  /** Sets asynchronous IO on or off. */
  def withAsyncIo(async: Boolean = true) : this.type = { this.useAsyncIo = async; this; }

  /** Sets asynchronous IO on or off. */
  def withIndexing(indexing: Boolean = true) : this.type = { this.createIndex = Some(indexing); this; }

}
