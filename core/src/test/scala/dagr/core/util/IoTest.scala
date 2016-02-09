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

import java.nio.file.{Files, Path}

/**
 * Tests for various methods in the Io class
 */
class IoTest extends UnitSpec {
  /** Creates a random tmp file that is deleted on exit. */
  def tmpfile(readable: Boolean = true, writable: Boolean = true, executable: Boolean = true) : Path = {
    permission(Files.createTempFile("foo", "bar"), readable, writable, executable)
  }

  /** Creates a random tmp directory and sets it to be deleted on exit. */
  def tmpdir(readable: Boolean = true, writable: Boolean = true, executable: Boolean = true) : Path = {
    permission(Files.createTempDirectory("foo-dir"), readable, writable, executable)
  }

  /** Sets permissions on a path. */
  def permission(path: Path, readable: Boolean = true, writable: Boolean = true, executable: Boolean = true) : Path = {
    val file = path.toFile
    file.setReadable(readable, false)
    file.setWritable(writable, false)
    file.setExecutable(executable, false)
    file.deleteOnExit()
    path
  }

  "Io.assertReadable" should "not throw an exception for extent files" in {
    val f1 = tmpfile(); val f2 = tmpfile(); val f3 = tmpfile()
    Io.assertReadable(f1)
    Io.assertReadable(List(f1, f2, f3))
  }

  it should "not throw an exception for special files" in {
    Io.assertReadable(Io.StdIn)
  }

  it should "throw an exception for when file isn't readable" in {
    val nullpath: Path = null
    an[IllegalArgumentException] should be thrownBy { Io.assertReadable(nullpath) }
    an[IllegalArgumentException] should be thrownBy { Io.assertReadable(List(nullpath)) }
    an[IllegalArgumentException] should be thrownBy { Io.assertReadable(Some(nullpath)) }
    an[AssertionError] should be thrownBy {Io.assertReadable(tmpdir())}
    an[AssertionError] should be thrownBy {Io.assertReadable(tmpfile(readable=false))}
  }

  "Io.assertListable" should "not throw an exception for extent dirs" in {
    val f1 = tmpdir(); val f2 = tmpdir(); val f3 = tmpdir()
    Io.assertListable(f1)
    Io.assertListable(List(f1, f2, f3))
  }

  it should "not throw an exception when a directory isn't listable" in {
    val nullpath: Path = null
    an[IllegalArgumentException] should be thrownBy { Io.assertListable(nullpath) }
    an[IllegalArgumentException] should be thrownBy { Io.assertListable(List(nullpath)) }
    an[IllegalArgumentException] should be thrownBy { Io.assertListable(Some(nullpath)) }
    an[AssertionError] should be thrownBy {Io.assertListable(tmpfile())}
    an[AssertionError] should be thrownBy {Io.assertListable(tmpdir(readable=false))}
    an[AssertionError] should be thrownBy {Io.assertListable(tmpdir(executable=false))}
    an[AssertionError] should be thrownBy {Io.assertListable(tmpdir(readable=false, executable=false))}
  }

  "Io.assertCanWriteFile" should "throw an exception because the parent directory does not exist" in {
    an[AssertionError] should be thrownBy Io.assertCanWriteFile(PathUtil.pathTo("/path/to/nowhere"))
  }

  it should "throw an exception because the parent exits and is not a directory" in {
    val f = tmpfile()
    an[AssertionError] should be thrownBy Io.assertCanWriteFile(PathUtil.pathTo(f.toAbsolutePath.toString, "/parent_is_file"))
  }

  it should "throw an exception because the parent directory is not writable" in {
    val dir = tmpdir(writable=false);
    an[AssertionError] should be thrownBy Io.assertCanWriteFile(dir)
  }

  "Io.assertWritableDirectory" should "succeed in assessing writability of a directory" in {
    val dir = tmpdir();
    Io.assertWritableDirectory(List(dir))
  }

  it should "throw an exception because the parent does not exist" in {
    an[AssertionError] should be thrownBy { Io.assertWritableDirectory(PathUtil.pathTo("/path/to/nowhere")) }
  }

  it should "throw an exception because the parent directory is not writable" in {
    val dir = tmpdir(writable=false);
    an[AssertionError] should be thrownBy Io.assertWritableDirectory(dir)
  }

  it should "throw an exception because the path was null" in {
    val nullpath: Path = null
    an[IllegalArgumentException] should be thrownBy Io.assertWritableDirectory(nullpath)
  }

  "Io.mkdirs" should "be able to create a directory" in {
    val dir = tmpdir()
    Files.delete(dir)
    Io.mkdirs(dir) shouldBe true
    Files.delete(dir)
  }

  "Io.findFirstExtentParent" should "find the first extant parent" in {
    val dir = tmpdir()
    val child = PathUtil.pathTo(dir.toAbsolutePath.toString, "child")
    Io.findFirstExtentParent(child).get.toAbsolutePath.toString shouldBe dir.toAbsolutePath.toString
    val grandchild = PathUtil.pathTo(dir.toAbsolutePath.toString, "grand/child")
    Io.findFirstExtentParent(grandchild).get.toAbsolutePath.toString shouldBe dir.toAbsolutePath.toString
  }
}
