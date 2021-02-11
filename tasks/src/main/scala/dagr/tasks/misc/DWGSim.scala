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
import dagr.tasks.DagrDef.{PathToFasta, PathToFastq, PathToIntervals, PathToVcf}

import scala.collection.mutable.ListBuffer

/**
  * Class for running DWGSim to generate simulated sequencing data
  */
class DWGSim(val vcf: PathToVcf,
             val fasta: PathToFasta,
             val outPrefix: Path,
             val depth: Int = 100,
             val iSize: Int = 350,
             val iSizeDev: Int = 100,
             val readLength: Int = 151,
             val randomSeed: Int = 42, // -1 to use current time
             val coverageTarget: PathToIntervals

            ) extends ProcessTask with Configuration with FixedResources {

  requires(Cores(1), Memory("2G"))
  private val dwgsim: Path = configureExecutable("dwgsim.executable", "dwgsim")

  val outputPairedFastq: PathToFastq = outPrefix.getParent.resolve(outPrefix.getFileName + ".bfast.fastq")

  override def args: Seq[Any] = {
    val buffer = ListBuffer[Any]()
    buffer ++= dwgsim :: Nil
    buffer ++= "-v" :: vcf :: Nil
    buffer ++= "-d" :: iSize :: Nil
    buffer ++= "-s" :: iSizeDev :: Nil
    buffer ++= "-C" :: depth :: Nil
    buffer ++= "-1" :: readLength :: Nil
    buffer ++= "-2" :: readLength :: Nil
    buffer ++= "-z" :: randomSeed :: Nil
    buffer ++= "-x" :: coverageTarget :: Nil
    buffer ++= fasta :: outPrefix :: Nil //these two arguments must be last
    buffer.toList
  }
}
