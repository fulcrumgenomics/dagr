/*
 * The MIT License
 *
 * Copyright (c) 2015-2016 Fulcrum Genomics LLC
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
package dagr.commons.util

import java.io.{ByteArrayOutputStream, PrintStream}

/** Methods to help capture stdin and stderr */
trait CaptureSystemStreams {

  /**
    * captures [[System.out]] while runnable is executing
    * @param runnable a code block to execute
    * @return everything written to [[System.out]] by runnable
    */
  def captureStdout(runnable: () => Unit): String = {
    captureSystemStream(runnable, System.out, (out: PrintStream) => System.setOut(out))
  }

  /**
    * captures [[System.err]] while runnable is executing
    * @param runnable a code block to execute
    * @return everything written to [[System.err]] by runnable
    */
  def captureStderr(runnable: () => Unit): String = {
    captureSystemStream(runnable, System.err, (out: PrintStream) => System.setErr(out))
  }

  private def captureSystemStream(runnable: () => Unit, stream: PrintStream, setterMethod: (PrintStream) => Unit): String = {
    val out: ByteArrayOutputStream = new ByteArrayOutputStream
    setterMethod(new PrintStream(out))
    try {
      runnable()
    } finally {
      setterMethod(stream)
    }
    out.toString
  }
}

