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
package dagr.commons.io

import java.io._
import java.nio.file.{Files, Path}

/**
 * IO Utility class for working with Path objects.
 */
object Io {
  val StdIn   = PathUtil.pathTo("/dev/stdin")
  val StdOut  = PathUtil.pathTo("/dev/stdout")
  val DevNull = PathUtil.pathTo("/dev/null")

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
    if (path == null)                throw new IllegalArgumentException("Cannot check readability of null path.")
    assert(!Files.notExists(path),   "Cannot read non-existent path: " + path)
    assert(!Files.isDirectory(path), "Cannot read path because it is a directory: " + path)
    assert(Files.isReadable(path),   "Path exists but is not readable: " + path)
  }

  /** Asserts that the Paths represent directories that can be listed. */
  def assertListable(paths : TraversableOnce[_ <: Path]) : Unit = paths.foreach(assertListable)

  /** Asserts that the Path represents a directory that can be listed. */
  def assertListable(path: Path) : Unit = {
    if (path == null)                throw new IllegalArgumentException("Cannot check readability of null path.")
    assert(!Files.notExists(path),   "Cannot read non-existent path: " + path)
    assert(Files.isDirectory(path),  "Cannot read path as file because it is a directory: " + path)
    assert(Files.isReadable(path),   "Directory exists but is not readable: " + path)
    assert(Files.isExecutable(path), "Directory exists but is not readable: " + path)
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
    if (path == null) throw new IllegalArgumentException("Cannot check writability of null path.")
    if (Files.exists(path)) {
      assert(Files.isWritable(path),   "File exists but is not writable: " + path)
      assert(!Files.isDirectory(path), "Cannot write file because it is a directory: " + path)
    }
    else {
      val absolute = path.toAbsolutePath
      val maybeParent = if (parentMustExist) Option(absolute.getParent) else findFirstExtentParent(absolute)
      maybeParent match {
        case None => assert(false,          "Cannot write file because parent directory does not exist: " + path)
        case Some(parent) =>
          assert(!Files.notExists(parent),  "Cannot write file because parent directory does not exist: " + path)
          assert(Files.isDirectory(parent), "Cannot write file because parent exits and is not a directory: " + path)
          assert(Files.isWritable(parent),  "Cannot write file because parent directory is not writable: " + path)
      }
    }
  }

  /** Asserts that a path represents an existing directory and that new files can be created within the directory. */
  def assertWritableDirectory(paths : TraversableOnce[_ <: Path]) : Unit = paths.foreach(assertWritableDirectory)

  /** Asserts that a path represents an existing directory and that new files can be created within the directory. */
  def assertWritableDirectory(path : Path) : Unit = {
    if (path == null)                throw new IllegalArgumentException("Cannot check readability of null path.")
    assert(!Files.notExists(path),  "Path does not exist: " + path)
    assert(Files.isDirectory(path), "Cannot write to path because it is not a directory: " + path)
    assert(Files.isWritable(path),  "Directory exists but is not writable: " + path)
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
  private[io] def findFirstExtentParent(p: Path) : Option[Path] = {
    p.getParent match {
      case null => None
      case parent if Files.exists(parent) => Some(parent)
      case parent => findFirstExtentParent(parent)
    }
  }

  /** Writes one or more lines to a file represented by a path. */
  def writeLines(path: Path, lines: Seq[String]) = {
    val writer = Io.toWriter(path)
    lines.foreach(line => writer.append(line).append('\n'))
    writer.close()
  }
}
