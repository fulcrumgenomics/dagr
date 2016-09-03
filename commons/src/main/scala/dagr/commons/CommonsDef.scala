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

package dagr.commons

import java.io.Closeable

import dagr.commons.util.Logger

import scala.util.{Failure, Success, Try}

/**
  * Object that is designed to be imported with `import CommonsDef._` in any/all classes
  * much like the way that scala.PreDef is imported in all files automatically.
  *
  * New methods, types and objects should not be added to this class lightly as they
  * will pollute the namespace of any classes which import it.
  */
class CommonsDef {
  /** An exception that implies that code is unreachable. */
  private class UnreachableException(message: String) extends IllegalStateException(message)

  /**
    * A terse way to throw an `UnreachableException` that can be used where any type is expected,
    * e.g. `Option(thing) getOrElse unreachable("my thing is never null")`
    *
    * @param message an optional message
    */
  def unreachable(message: => String = ""): Nothing = throw new UnreachableException(message)


  /**
    * Construct to capture a value, execute some code, and then returned the captured value. Allows
    * code like:
    *   `val x = foo; foo +=1; return x`
    * to be replaced with
    *   `yieldAndThen(foo) {foo +=1}`
    *
    * @param it the value to be returned/yielded
    * @param block a block of code to be evaluated
    * @tparam A the type of thing to be returned (usually inferred)
    * @return it
    */
  def yieldAndThen[A](it: => A)(block: => Unit): A = {
    val retval : A = it
    block
    retval
  }

  /**
   * Implicit class that wraps a closeable and provides a safelyClose method
   * that will not throw any exception.
   */
  implicit class SafelyClosable(private val c: AutoCloseable) {
    def safelyClose() : Unit = {
      try { c.close() }
      catch { case ex: Exception => Unit }
    }
  }

  /** Performs the unit of work on a closeable resource of type [[A]] closing up the resource in the case of an
    * exception or upon completion, and ultimately returning a [[Try]].  An exception during closing does not change the
    * success of the work.  If a logger is provided, the exception is logged, otherwise the exception is ignored.
    *
    * An example would be:
    * {{{
    *   tryWith(Io.toWriter("/path/does/not/exists") { writer => writer.write("Hello World!")) }
    * }}}
    * which throws an exception since the path is not found, but this exception is ignored, and a [[Failure]] is
    * returned.
    *
    * @param resource the resource upon which work is performed.
    * @param doWork the work to perform on the resource.
    * @tparam A the resource type.
    * @return [[Success]] if the work was performed successfully, [[Failure]] otherwise.
    */
  def tryWithCloseable[A <: Closeable](resource: A, logger: Option[Logger] = None)(doWork: A => Unit): Try[Unit] = {
    tryWith(resource, logger)(_.close())(doWork)
  }

  /** Performs the unit of work on a resource of type [[A]] cleaning up the resource in the case of an exception or upon
    * completion, and ultimately returning a [[Try]] of type [[B]].  An exception during cleanup does not change the success
    * of the work.  If a logger is provided, the exception is logged, otherwise the exception is ignored.
    *
    * An example would be:
    * {{{
    *   tryWith(Io.toWriter("/path/does/not/exists")(_.close()) { writer => writer.write("Hello World!")) }
    * }}}
    * which throws an exception since the path is not found, but this exception is ignored, and a [[Failure]] is
    * returned.
    *
    * @param resource the resource upon which work is performed.
    * @param cleanup the clean up method to apply to the resource when the work is complete.
    * @param doWork the work to perform on the resource, returning a result of type [[B]].
    * @tparam A the resource type.
    * @tparam B the result type of the work performed.
    * @return [[Success]] if the work was performed successfully, [[Failure]] otherwise.
    */
  def tryWith[A, B](resource: A, logger: Option[Logger] = None)(cleanup: A => Unit)(doWork: A => B): Try[B] = {
    try {
      Success(doWork(resource))
    } catch {
      case e: Throwable => Failure(e)
    }
    finally {
      try {
        if (resource != null) cleanup(resource)
      } catch {
        case e: Exception => logger.foreach(_.exception(e))
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////
  // Path-like typedefs that are used to hint at what the Path should be to
  /////////////////////////////////////////////////////////////////////////////

  /** Represents a path to a BAM (or SAM or CRAM) file. */
  type PathToBam = java.nio.file.Path

  /** Represents a path to an intervals file (IntervalList or BED). */
  type PathToIntervals = java.nio.file.Path

  /** Represents a path to a FASTQ file (optionally gzipped). */
  type PathToFastq = java.nio.file.Path

  /** Represents a path to a Reference FASTA file. */
  type PathToFasta = java.nio.file.Path

  /** Represents a path to a VCF/BCF/VCF.gz. */
  type PathToVcf = java.nio.file.Path

  /** Represents a full path including directories, that is intended to be used as a prefix for generating file paths. */
  type PathPrefix = java.nio.file.Path

  /** Represents a path to directory. */
  type DirPath = java.nio.file.Path

  /** Represents a path to a file (not a directory) that doesn't have a more specific type. */
  type FilePath = java.nio.file.Path

  /** A String that represents the prefix or basename of a filename. */
  type FilenamePrefix = String
}

/** A singleton object providing access to all the functinoality of CommonsDef. */
object CommonsDef extends CommonsDef
