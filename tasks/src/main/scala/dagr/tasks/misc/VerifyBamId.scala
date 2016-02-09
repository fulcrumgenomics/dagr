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

import java.nio.file.Path

import dagr.core.config.Configuration
import dagr.core.execsystem.{Cores, Memory}
import dagr.core.tasksystem.{FixedResources, ProcessTask}
import dagr.tasks.{FilenamePrefix, PathToBam, PathToVcf}

import scala.collection.mutable.ListBuffer

/**
  * Class for running verifyBamID on sequence data in genotype-free mode.
  */
class VerifyBamId(val vcf: PathToVcf,
                  val bam: PathToBam,
                  val out: Path,
                  val maxDepth: Int = 50,
                  val minMapq: Int = 20,
                  val minQ: Int = 20,
                  val grid: Double = 0.01,
                  val precise: Boolean = true
                 ) extends ProcessTask with Configuration with FixedResources {

  requires(Cores(1), Memory("1G"))
  private val verifyBamID: Path = configureExecutable("verifybamid.executable", "verifyBamID")


  override def args: Seq[Any] = {
    val buffer = ListBuffer[Any]()
    buffer ++= verifyBamID :: "--ignoreRG" :: "--chip-none" :: "--noPhoneHome" :: "--self" :: Nil
    buffer ++= "--vcf" :: vcf :: "--bam " :: bam :: "--out" :: out :: Nil
    buffer ++= "--maxDepth" :: maxDepth :: "--minMapQ" :: minMapq :: "--minQ" :: minQ :: "--grid" :: grid :: Nil
    if (precise) buffer += "--precise"
    buffer.toList
  }
}
