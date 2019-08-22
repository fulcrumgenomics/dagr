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

import java.nio.file.{Files, Path}
import java.util.jar.Attributes.Name
import java.util.jar.JarInputStream

import dagr.core.config.Configuration
import dagr.core.execsystem.{Cores, Memory}
import dagr.core.tasksystem.{FixedResources, ProcessTask}
import dagr.tasks.JarTask
import com.fulcrumgenomics.commons.CommonsDef._

import scala.collection.mutable.ListBuffer

object GatkTask {
  val GatkJarPathConfigKey = "gatk.jar"

  private[gatk] val ImplementationVersion = new Name("Implementation-Version")
  private[gatk] val MainClass             = new Name("Main-Class")

  /** Attempts to determine the major version from the GATK Jar. */
  def majorVersion(jar: FilePath): Int = {
    val in    = new JarInputStream(Files.newInputStream(jar))
    val attrs = in.getManifest.getMainAttributes
    in.close()

    if (attrs.containsKey(ImplementationVersion)) {
      attrs.getValue(ImplementationVersion).takeWhile(_ != '.').toInt
    }
    else {
      attrs.getValue(MainClass) match {
        case "org.broadinstitute.sting.gatk.CommandLineGATK"  => 1
        case "org.broadinstitute.gatk.engine.CommandLineGATK" => 3
        case x => throw new IllegalArgumentException(s"Couldn't determine GATK version from jar $jar")
      }
    }
  }

  /** The path to the "standard" GATK jar. */
  lazy val GatkJarPath: FilePath = Configuration.configure[Path](GatkTask.GatkJarPathConfigKey)

  /** The major version of the "standard" GATK jar. */
  lazy val GatkMajorVersion: Int = majorVersion(GatkJarPath)
}


/**
  * Abstract base class for tasks that involve running the GATK.
  */
abstract class GatkTask(val walker: String,
                        val ref: PathToFasta,
                        val intervals: Option[PathToIntervals] = None,
                        val bamCompression: Option[Int] = None)
  extends ProcessTask with JarTask with FixedResources with Configuration {
  requires(Cores(1), Memory("4g"))

  override def args: Seq[Any] = {
    val buffer  = ListBuffer[Any]()
    val jvmArgs = if (gatkMajorVersion >= 4) bamCompression.map(c => s"-Dsamjdk.compression_level=$c") else None
    buffer.appendAll(jarArgs(this.gatkJar, jvmMemory=this.resources.memory, jvmArgs=jvmArgs))

    if (gatkMajorVersion < 4) buffer += "-T"
    buffer += this.walker

    if (gatkMajorVersion < 4) bamCompression.foreach(c => buffer.append("--bam_compression", c))

    buffer.append("-R", this.ref.toAbsolutePath.toString)
    intervals.foreach(il => buffer.append("-L", il.toAbsolutePath.toString))

    addWalkerArgs(buffer)
    buffer.toSeq
  }

  /** Can be overridden to use a specific GATK jar. */
  protected def gatkJar: Path = configure[Path](GatkTask.GatkJarPathConfigKey)

  /** The version of GATK being used by this task. */
  protected lazy val gatkMajorVersion: Int = {
    val jar = gatkJar
    if (jar == GatkTask.GatkJarPath) GatkTask.GatkMajorVersion
    else GatkTask.majorVersion(jar)
  }

  /** Adds arguments specific to the walker. */
  protected def addWalkerArgs(buffer: ListBuffer[Any]): Unit
}
