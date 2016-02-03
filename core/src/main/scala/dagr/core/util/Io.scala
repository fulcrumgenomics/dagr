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
package dagr.core.util

import java.io._
import java.nio.file.{Files, Path, Paths}

/**
 * IO Utility class for working with Path objects.
 */
object Io {
  val StdIn   = Paths.get("/dev/stdin")
  val StdOut  = Paths.get("/dev/stdout")
  val DevNull = Paths.get("/dev/null")

  /** Creates a new BufferedWriter to write to the supplied path. */
  def toWriter(path: Path) : BufferedWriter = Files.newBufferedWriter(path)

  /** Creates a new BufferedReader to read from the supplied path. */
  def toReader(path: Path) : BufferedReader = Files.newBufferedReader(path)

  /** Creates a new InputStream to read from the supplied path. */
  def toInputStream(path: Path) : InputStream = new BufferedInputStream(Files.newInputStream(path))

  /** Creates a new BufferedReader to read from the supplied path. */
  def toOutputStream(path: Path) : OutputStream = new BufferedOutputStream(Files.newOutputStream(path))

  /** Makes a new temporary directory. */
  def makeTempDir(name: String) : Path = Files.createTempDirectory(name)

  /** Asserts that the Path represents a file that can be opened and read. */
  def assertReadable(paths : TraversableOnce[_ <: Path]) : Unit = paths.foreach(assertReadable)

  /** Asserts that the Paths represents files that can be opened and read. */
  def assertReadable(path: Path) : Unit = {
    if (path == null)            throw new IllegalArgumentException("Cannot check readability of null path.")
    if (Files.notExists(path))   throw new AssertionError("Cannot read non-existent path: " + path)
    if (Files.isDirectory(path)) throw new AssertionError("Cannot read path because it is a directory: " + path)
    if (!Files.isReadable(path)) throw new AssertionError("Path exists but is not readable: " + path)
  }

  /** Asserts that the Paths represent directories that can be listed. */
  def assertListable(paths : TraversableOnce[_ <: Path]) : Unit = paths.foreach(assertListable)

  /** Asserts that the Path represents a directory that can be listed. */
  def assertListable(path: Path) : Unit = {
    if (path == null)              throw new IllegalArgumentException("Cannot check readability of null path.")
    if (Files.notExists(path))     throw new AssertionError("Cannot read non-existent path: " + path)
    if (!Files.isDirectory(path))  throw new AssertionError("Cannot read path as file because it is a directory: " + path)
    if (!Files.isReadable(path))   throw new AssertionError("Directory exists but is not readable: " + path)
    if (!Files.isExecutable(path)) throw new AssertionError("Directory exists but is not readable: " + path)
  }

  /**
    * Asserts that it will be possible to write to a file at Path, possibly after creating parent directories.
    *
    * @param paths one or more paths to check
    * @param parentMustExist if true (default) the file or its direct parent must exist, if false then only
    *                        require that the first parent that actually exists is writable
    */
  def assertCanWriteFiles(paths : TraversableOnce[_ <: Path], parentMustExist:Boolean = true) : Unit = {
    paths.foreach(p => assertCanWriteFile(p, parentMustExist))
  }

  /**
    * Asserts that it will be possible to write to a file at Path, possibly after creating parent directories.
    *
    * @param path the path to check
    * @param parentMustExist if true (default) the file or its direct parent must exist, if false then only
    *                        require that the first parent that actually exists is writable
    */
  def assertCanWriteFile(path: Path, parentMustExist: Boolean=true) : Unit = {
    if (path == null) throw new IllegalArgumentException("Cannot check readability of null path.")

    if (Files.exists(path)) {
      if (!Files.isWritable(path)) throw new AssertionError("File exists but is not writable: " + path)
      if (Files.isDirectory(path)) throw new AssertionError("Cannot write file because it is a directory: " + path)
    }
    else {
      val absolute = path.toAbsolutePath
      val maybeParent = if (parentMustExist) Option(absolute.getParent) else findFirstExtentParent(absolute)
      maybeParent match {
        case None => throw new AssertionError("Cannot write file because parent directory does not exist: " + path)
        case Some(parent) =>
          if (Files.notExists(parent))      throw new AssertionError("Cannot write file because parent directory does not exist: " + path)
          if (!Files.isDirectory(parent))   throw new AssertionError("Cannot write file because parent exits and is not a directory: " + path)
          if (!Files.isWritable(parent))    throw new AssertionError("Cannot write file because parent directory is not writable: " + path)
      }
    }
  }

  /** Asserts that a path represents an existing directory and that new files can be created within the directory. */
  def assertWritableDirectory(paths : TraversableOnce[_ <: Path]) : Unit = paths.foreach(assertWritableDirectory)


  /** Asserts that a path represents an existing directory and that new files can be created within the directory. */
  def assertWritableDirectory(path : Path) : Unit = {
    if (path == null)             throw new IllegalArgumentException("Cannot check readability of null path.")
    if (Files.notExists(path))    throw new AssertionError("Path does not exist: " + path)
    if (!Files.isDirectory(path)) throw new AssertionError("Cannot write to path because it is not a directory: " + path)
    if (!Files.isWritable(path))  throw new AssertionError("Directory exists but is not writable: " + path)
  }

  /**
    * Method that attempts to create a directory and all it's parents.
    *
    * @return true if the directory exists after the call, false otherwise
    */
  def mkdirs(path: Path): Boolean = {
    try {
      Files.createDirectories(path)
      Files.exists(path)
    }
    catch  {
      case ex: IOException => false
    }
  }

  /** Works its way up a path finding the first parent path that actually exists. */
  private def findFirstExtentParent(p: Path) : Option[Path] = {
    val parent = p.getParent
    if (parent == null) None
    else if (Files.exists(parent)) Some(parent)
    else findFirstExtentParent(parent)
  }
}
