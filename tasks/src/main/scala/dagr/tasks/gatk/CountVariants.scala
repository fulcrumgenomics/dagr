/*
 * The MIT License
 *
 * Copyright (c) 2021 Fulcrum Genomics LLC
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

import java.lang.Long
import java.nio.file.Path

import dagr.tasks.DagrDef.PathToVcf

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * Runs the GATK walker that counts variants
  */
class CountVariants(val in: PathToVcf, val output: Path)
  extends Gatk4Task(walker = "CountVariants") {

  var count = 0L

  override protected def addWalkerArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("-V", in)
    buffer.append("-O", output)
  }

  /** Finalize anything after the task has been run.
    *
    * This method should be called after a task has been run. The intended use of this method
    * is to allow for any modification of this task prior to any dependent tasks being run.  This
    * would allow any parameters that were passed to dependent tasks as call-by-name to be
    * finalized here.  For example, we could have passed an Option[String] that is None
    * until make it Some(String) in this method.  Then when the dependent task's getTasks
    * method is called, it can call 'get' on the option and get something.
    *
    * @param exitCode the exit code of the task, which could also be 1 due to the system terminating this process
    * @return true if we c
    */
  override def onComplete(exitCode: Int): Boolean = {

    val source = Source.fromFile(output.toFile)

    try {
      count = Long.parseLong(source.getLines.mkString)
    } finally {
      source.close()
    }

    super.onComplete(exitCode)
  }
}
