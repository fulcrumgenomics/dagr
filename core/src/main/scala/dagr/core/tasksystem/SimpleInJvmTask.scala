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

import dagr.core.execsystem.{Cores, Memory}

/** Companion object for SimpleInJvmTask that provides helpful factory methods. */
object SimpleInJvmTask {
  /** Creates a new SimpleInJvmTask that will execute the provided function when invoked. */
  def apply(f: => Unit) : SimpleInJvmTask = {
    new SimpleInJvmTask { override def run(): Unit = f }
  }

  /** Creates a new SimpleInJvmTask that will execute the provided function when invoked, and gives it a name. */
  def apply(name: String, f: => Unit) : SimpleInJvmTask = {
    apply(f).withName(name)
  }
}

/**
  * A simplified In JVM task that hides the need to return an Int exit code and instead
  * uses the raising of exceptions or lack thereof to indicate failure and success.
  */
abstract class SimpleInJvmTask extends InJvmTask with FixedResources {
  name = "SimpleInJvmTask"
  requires(Cores(1), Memory("32M"))

  /** Executes run() and returns 0 if no exception is thrown, otherwise 1. */
  override def inJvmMethod(): Int = {
    try {
      run()
      0
    }
    catch { case t:Throwable =>
      logger.exception(t, "Exception while processing in JVM task ", name)
      1
    }
  }

  /**
    * Abstract method to be implemented by subclasses to perform any tasks. Should throw an exception
    * to indicate an error or problem during processing.
    */
  def run() : Unit
}
