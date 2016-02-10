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
package dagr.core.tasksystem

import java.io.PrintStream
import java.nio.file.Path

/** A task that can execute a method in the Jvm, and does not generate any new tasks. */
abstract class InJvmTask extends UnitTask {

  /** The method to execute in the Jvm
   *
   * Do not override this (you cannot anyway), as this sets the logging correctly for this task.
   * Override [[inJvmMethod()]] instead.
   *
   * @return the equivalent of an exit code, with zero being success
   */
  final def inJvmMethod(script: Path, logFile: Path): Int = {
    val writer = new PrintStream(logFile.toFile)
    logger.out = Some(writer)
    val exitCode = inJvmMethod()
    writer.close()
    logger.out = None
    exitCode
  }

  /** The method to execute in the Jvm
    *
    * @return the equivalent of an exit code, with zero being success
    */
  def inJvmMethod(): Int
}
