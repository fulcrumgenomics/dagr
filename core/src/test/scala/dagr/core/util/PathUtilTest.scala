/*
 * The MIT License
 *
 * Copyright (c) 2015-6 Fulcrum Genomics LLC
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

import java.nio.file.Paths

/** Tests for the PathUtil. */
class PathUtilTest extends UnitSpec {

  "PathUtil.removeExtension" should " correctly remove a single extension from a filename" in {
    PathUtil.removeExtension("foo.bam") should be ("foo")
    PathUtil.removeExtension("really_long_name_here.with_a_really_long_ext_here") should be ("really_long_name_here")
  }

 it should " correctly remove only the last extention in a filename" in {
    PathUtil.removeExtension("foo.bar.splat.bam") should be ("foo.bar.splat")
    PathUtil.removeExtension("a.b.c.d.e.f.g.h.i.j.k.l.m") should be ("a.b.c.d.e.f.g.h.i.j.k.l")
  }

 it should " not alter filenames without periods in them" in {
    PathUtil.removeExtension("foo_bar") should be ("foo_bar")
    PathUtil.removeExtension("foo-bar-splat") should be ("foo-bar-splat")
  }

 it should " not return empty strings for filenames that start with a period" in {
    PathUtil.removeExtension(".foo") should be (".foo")
    PathUtil.removeExtension(".really_long_hiding_place") should be (".really_long_hiding_place")
  }

 it should " handle names with path elements" in {
    PathUtil.removeExtension("/foo/bar/splat/foo.txt")  should be ("/foo/bar/splat/foo")
    PathUtil.removeExtension("/foo/bar/.splat/foo.txt") should be ("/foo/bar/.splat/foo")
    PathUtil.removeExtension("/foo/bar/.splat")         should be ("/foo/bar/.splat")
    PathUtil.removeExtension("/foo/bar/.splat/foo")     should be ("/foo/bar/.splat/foo")
    PathUtil.removeExtension("/foo/bar/splat/foo.txt")  should be ("/foo/bar/splat/foo")
  }

  "PathUtil.basename" should " correctly remove leading path and extension if present" in {
    PathUtil.basename(".foo", trimExt=true) should be (".foo")
    PathUtil.basename("/foo/bar/splat/wheee.txt", trimExt=true) should be ("wheee")
    PathUtil.basename("/foo/bar/splat/wheee.txt", trimExt=false) should be ("wheee.txt")
    PathUtil.basename("wibble.txt", trimExt=true) should be ("wibble")
  }

  "PathUtil.sanitizeFileName" should "should replace illegal characters with underscores" in {
    PathUtil.sanitizeFileName("A B!C") should be ("A_B_C")
    PathUtil.sanitizeFileName("A_B!C", replacement = Some('X')) should be ("A_BXC")
    PathUtil.sanitizeFileName("A B\nC") should be ("A_B\nC")
    PathUtil.sanitizeFileName("A1B2C", replacement = Some('_')) should be ("A1B2C")
    PathUtil.sanitizeFileName("A_B C", illegalCharacters = s"${PathUtil.illegalCharacters}ABC", replacement = Some('_')) should be ("_____")
  }

  it should "should replace extensions" in {
    PathUtil.replaceExtension(PathUtil.pathTo("Foo.bam"), ".bai") should be (PathUtil.pathTo("Foo.bai"))
    PathUtil.replaceExtension(PathUtil.pathTo("Foo.bar.bam"), ".bai") should be (PathUtil.pathTo("Foo.bar.bai"))
    PathUtil.replaceExtension(PathUtil.pathTo("/Foo/bar/splat.bam"), ".not_yo_mama") should be (PathUtil.pathTo("/Foo/bar/splat.not_yo_mama"))
  }

  "PathUtil.pathTo" should "resolve absolute paths just like Paths.get does" in {
    PathUtil.pathTo("/foo")         shouldBe Paths.get("/foo")
    PathUtil.pathTo("/foo/bar")     shouldBe Paths.get("/foo/bar")
    PathUtil.pathTo("/foo/bar.txt") shouldBe Paths.get("/foo/bar.txt")
    PathUtil.pathTo("/foo", "bar")  shouldBe Paths.get("/foo/bar")
  }

  it should "make relative paths absolute" in {
    val cwd = Paths.get(".").toAbsolutePath.normalize
    PathUtil.pathTo("relative/path.txt") shouldBe cwd.resolve("relative/path.txt")
    PathUtil.pathTo("../relative/path2.txt") shouldBe cwd.getParent.resolve("relative/path2.txt")
  }

  "PathUtil.extensionOf" should "return the correct extension when present" in {
    PathUtil.extensionOf(Paths.get("foo.txt")) shouldBe Some(".txt")
    PathUtil.extensionOf(PathUtil.pathTo("foo.txt")) shouldBe Some(".txt")
    PathUtil.extensionOf(PathUtil.pathTo("/foo/bar.splat/one.two.three.four")) shouldBe Some(".four")
    PathUtil.extensionOf(PathUtil.pathTo("/foo/bar.splat/.one")) shouldBe Some(".one")
  }

  it should "return None when there is no extension" in {
    PathUtil.extensionOf(Paths.get("foo")) shouldBe None
    PathUtil.extensionOf(PathUtil.pathTo("foo")) shouldBe None
    PathUtil.extensionOf(PathUtil.pathTo("/foo/bar/.splat/my.dir/foo")) shouldBe None
  }
}
