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

package dagr.core.cmdline

import java.io.PrintWriter
import java.nio.file.{Files, Path}

import com.fulcrumgenomics.commons.CommonsDef._
import com.fulcrumgenomics.commons.util.ClassFinder
import dagr.core.UnitSpec
import dagr.core.tasksystem.Pipeline
import org.reflections.util.ClasspathHelper

object DagrScriptManagerTest {
  def packageName: String = "dagr.example"

  def helloWorldScript(packageName: String = packageName) =
    s"""
      |package ${packageName}
      |
      |import com.fulcrumgenomics.sopt._
      |import dagr.core.tasksystem.Pipeline
      |
      |// NB: output directories must exist
      |@clp(description = "Hello World Pipeline")
      |class HelloWorldPipeline
      |(
      |  @arg(doc = "First set of text")
      |  var blockOne: List[String] = Nil,
      |  @arg(doc = "Second set of text")
      |  var blockTwo: List[String] = Nil
      |) extends Pipeline {
      |
      |  override def build(): Unit = {
      |      println("blockOne: " + blockOne.mkString("\\n"))
      |      println("blockTwo: " + blockTwo.mkString("\\n"))
      |  }
      |}
    """.stripMargin

  val buggyScript =
    """
      |package dagr.example
      |
      | Bug Bug Bug
      | Bug Bug Bug
      | BUg Bug Bug
      |
      | class flarfle extends noodle ()
    """.stripMargin

  def writeScript(content: String): (Path, Path) = {
    val tmpDir: Path = Files.createTempDirectory("dagrScriptDirectory")
    val tmpFile: Path = Files.createTempFile(tmpDir, "dagrScript", ".dagr")
    tmpDir.toFile.deleteOnExit()
    tmpFile.toFile.deleteOnExit()

    // write the script
    val writer = new PrintWriter(tmpFile.toFile)
    writer.println(content)
    writer.close()

    (tmpDir, tmpFile)
  }
}

class DagrScriptManagerTest extends UnitSpec {
  import DagrScriptManagerTest._

  "DagrScriptManager" should "compile and load a Dagr script" in {
    val (tmpDir: Path, tmpFile: Path) = writeScript(helloWorldScript())

    val manager = new DagrScriptManager
    manager.loadScripts(Seq(tmpFile), tmpDir)

    // make sure tmpDir is not on the classpath
    ClasspathHelper.forManifest.iterator.toSet.exists(url => url.toString.contains(tmpDir.toString)) shouldBe true

    // make sure we find the class in the classpath
    val classFinder: ClassFinder = new ClassFinder
    classFinder.find("dagr.example", classOf[Pipeline])
    classFinder
      .getClasses
      .iterator
      .map(_.getCanonicalName)
      .exists(name => 0 == name.compareTo("dagr.example.HelloWorldPipeline")) shouldBe true

  }

  it should "fail to compile a buggy Dagr script" in {
    val (tmpDir: Path, tmpFile: Path) = writeScript(buggyScript)

    val manager = new DagrScriptManager
    an[RuntimeException] should be thrownBy manager.loadScripts(Seq(tmpFile), tmpDir)

    // make sure tmpDir is not on the classpath
    ClasspathHelper.forManifest.iterator.exists(url => url.toString.contains(tmpDir.toString)) shouldBe false
  }
}
