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
package dagr.tasks.vc

import java.nio.file.Path

import dagr.core.config.Configuration
import dagr.api.models.{Cores, Memory}
import dagr.core.tasksystem.{FixedResources, ProcessTask}
import dagr.tasks.DagrDef.PathPrefix
import dagr.tasks.JarTask

import scala.collection.mutable.ListBuffer

object Varscan2 {
  val VarScan2JarPathConfigKey = "varscan2.jar"
}

/**
  * Task for running VarScan2's somatic calling from pileup.
  */
class Varscan2Somatic(val tumorPileup: Path,
                      val normalPileup: Path,
                      val out: PathPrefix,
                      val minimumVariantFrequency: Double = 0.01,
                      val pValue: Double = 0.2,
                      val somaticPValue: Double = 0.05,
                      val strandFilter: Boolean = true,
                      val vcfOutput: Boolean = true)
  extends ProcessTask with JarTask with FixedResources with Configuration {

  requires(Cores(1), Memory("4g"))
  def jar: Path = configure[Path](Varscan2.VarScan2JarPathConfigKey)

  override def args: Seq[Any] = {
    val buffer = ListBuffer[String]()
    buffer.appendAll(jarArgs(jarPath=jar, jvmMemory=this.resources.memory))
    buffer.append("somatic", normalPileup.toString, tumorPileup.toString, out.toString)
    buffer.append("--min-var-freq", minimumVariantFrequency.toString)
    buffer.append("--p-value", pValue.toString)
    buffer.append("--somatic-p-value", somaticPValue.toString)
    buffer.append("--strand-filter", if (strandFilter) "1" else "0")
    if (vcfOutput) buffer.append("--output-vcf", "1")
    buffer.toList
  }
}

