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
package dagr.core.tasksystem

import dagr.commons.io.{Io, PathUtil}
import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.commons.util.UnitSpec

/**
  * Tests for the piping together of tasks
  */
class PipeTest extends UnitSpec {
  val `|`   = Pipes.PipeString
  val `>`   = Pipes.RedirectOverwriteString
  val `>>` = Pipes.RedirectAppendString
  val `2>`  = Pipes.RedirectErrorOverwriteString
  val `2>>` = Pipes.RedirectErrorAppendString

  // Data types used in piping
  object Types {
    class Text
    class Binary
    class Csv extends Text
    class Tsv extends Text
  }

  // Some tasks used in piping
  import Types._
  val cores  = 0.25
  val bytes = Memory("16M").bytes
  trait TestResources extends FixedResources { requires(Cores(0.25), Memory(bytes)) }
  case class Cat(f: String) extends PipeOut[Text] with TestResources { def args = "cat" :: f.toString :: Nil }
  case class MakeCsv()  extends Pipe[Text,Csv] with TestResources { def args = "sed" :: "s/ +/,/g" :: Nil }
  case class CsvToTsv() extends Pipe[Csv,Tsv] with TestResources  { def args = "tr" :: "," :: """\t""" :: Nil }
  case class Column()   extends Pipe[Text,Text] with TestResources { def args = "column" :: "-t" :: Nil }
  case class BgZip()    extends Pipe[Text,Binary] with VariableResources {
    override def pickResources(available: ResourceSet): Option[ResourceSet] = available.subset(Cores(1), Cores(8), Memory(bytes))
    override def args: Seq[Any] = "bgzip" :: "--stdout" :: "--threads" :: resources.cores ::  Nil
  }
  case class BgUnzip()  extends Pipe[Binary,Text] with TestResources { def args = "bgzip" :: "--decompress" :: "--stdout" :: Nil  }

  "Pipe" should "connect some simple tasks" in {
    val pipe = Cat("foo.txt") | MakeCsv()
    pipe.commandLine shouldBe "cat foo.txt" + Pipes.PipeString + "sed 's/ +/,/g'"

    val pipe2 = Cat("bar.txt") | MakeCsv() | CsvToTsv() | Column()
    pipe2.commandLine shouldBe "cat bar.txt" + `|` + "sed 's/ +/,/g'" + `|` + """tr , '\t'""" + `|` + "column -t"
  }

  it should "handle re-use of parts of pipes" in {
    val start = Cat("foo.txt") | MakeCsv()
    val pipe1 = start | CsvToTsv()
    val pipe2 = start | Column()
    pipe1.commandLine shouldBe "cat foo.txt" + `|` + "sed 's/ +/,/g'" + `|` + """tr , '\t'"""
    pipe2.commandLine shouldBe "cat foo.txt" + `|` + "sed 's/ +/,/g'" + `|` + "column -t"
  }

  it should "correctly handle resources for several FixedResources tasks" in {
    val pipe1 = Cat("foo.txt") | MakeCsv()
    val pipe2 = pipe1 | CsvToTsv()
    val pipe3 = pipe2 | Column() | Column()

    pipe1.pickResources(ResourceSet.infinite) shouldBe Some(ResourceSet(2*cores, 2*bytes))
    pipe1.pickResources(ResourceSet.empty)    shouldBe None

    pipe2.pickResources(ResourceSet.infinite) shouldBe Some(ResourceSet(3*cores, 3*bytes))
    pipe2.pickResources(ResourceSet.empty)    shouldBe None

    pipe3.pickResources(ResourceSet.infinite) shouldBe Some(ResourceSet(5*cores, 5*bytes))
    pipe3.pickResources(ResourceSet.empty)    shouldBe None
  }

  it should "correctly handle pipes with a single variable resource task in them" in {
    val cat = Cat("stuff.csv")
    val csv = MakeCsv()
    val tsv = CsvToTsv()
    val zip = BgZip()

    val pipe = cat | csv | tsv | zip
    pipe.pickResources(ResourceSet.infinite) shouldBe Some(ResourceSet(3*cores + 8, 4*bytes))
    pipe.pickResources(ResourceSet(5, Long.MaxValue)) shouldBe Some(ResourceSet(4.75, 4*bytes))
    pipe.pickResources(ResourceSet.empty)    shouldBe None
    pipe.pickResources(ResourceSet(3*cores, 4*bytes)) shouldBe None // Enough for the fixed, but not the variable

    val picked = pipe.pickResources(ResourceSet.infinite)
    pipe.applyResources(picked.get)
    zip.resources shouldBe ResourceSet(8, bytes)

  }

  it should "throw an exception when constructing a pipe chain with multiple variable resource tasks" in {
    val start = Cat("stuff.csv") | MakeCsv() | CsvToTsv() | BgZip() | BgUnzip()
    an[IllegalArgumentException] shouldBe thrownBy { start | BgZip() }
  }

  it should "allow use of Pipe.empty anywhere in a pipeline" in {
    List(true, false).foreach(col => {
      val maybeColumn = if (col) new Column() else Pipes.empty[Text]
      val pipe = Cat("foo.txt") | maybeColumn | MakeCsv()
      pipe.commandLine shouldBe {
        if (col) "cat foo.txt" + `|` + "column -t" +  `|` +  "sed 's/ +/,/g'"
        else     "cat foo.txt" + `|` + "sed 's/ +/,/g'"
      }
    })

    val silly = Pipes.empty[Csv] | Pipes.empty[Csv] | Pipes.empty[Text]
    val pipe = Cat("foo.txt") | MakeCsv() | silly | Column()
    pipe.commandLine shouldBe "cat foo.txt" + `|` + "sed 's/ +/,/g'" + `|` + "column -t"
  }

  it should "construct the write command line when redirecting to a file at the end of a pipe" in {
    val cat = Cat("stuff.csv")
    val col = Column()
    val pipe1 = cat | col > PathUtil.pathTo("/tmp/foo.txt")
    pipe1.commandLine shouldBe "cat stuff.csv" + `|` + "column -t" + `>` + "/tmp/foo.txt"

    val pipe2 = cat | col >> PathUtil.pathTo("/tmp/append_to_me.txt")
    pipe2.commandLine shouldBe "cat stuff.csv" + `|` + "column -t" + `>>` + "/tmp/append_to_me.txt"
  }

  it should "construct the write command line when redirecting stderr anywhere in pipe" in {
    val cat = Cat("stuff.csv")
    val col = Column()
    val csv = MakeCsv()

    val pipe1 = cat.discardError() | col | csv > PathUtil.pathTo("/tmp/foo.txt")
    pipe1.commandLine shouldBe "cat stuff.csv" + `2>` + "/dev/null" + `|` + "column -t" + `|` + "sed 's/ +/,/g'" + `>` + "/tmp/foo.txt"

    val pipe2 = cat.>>!(Io.DevNull) | col | csv >> PathUtil.pathTo("/tmp/append_to_me.txt")
    pipe2.commandLine shouldBe "cat stuff.csv" + `2>>` + "/dev/null" + `|` + "column -t" + `|` + "sed 's/ +/,/g'" + `>>` + "/tmp/append_to_me.txt"

    val pipe3 = cat | col.>!(PathUtil.pathTo("/tmp/stderr.txt")) | csv >> PathUtil.pathTo("/tmp/append_to_me.txt")
    pipe3.commandLine shouldBe "cat stuff.csv" + `|` + "column -t" + `2>` + "/tmp/stderr.txt" + `|` + "sed 's/ +/,/g'" + `>>` + "/tmp/append_to_me.txt"
  }

  it should "compile with a subtype as input in a pipe" in {
    val pipe1 = Cat("foo.txt") | BgZip() // exact type: Text
    val pipe2 = Cat("foo.txt") | MakeCsv() | BgZip() // subtype: Csv
    val pipe3 = Cat("foo.txt") | MakeCsv() | CsvToTsv() | BgZip() // subtype: Tsv
  }

  "EmptyPipe" should "explode if its arg or pickResources methods are ever called" in {
    an[IllegalStateException] shouldBe thrownBy {Pipes.empty[Any].args }
    an[IllegalStateException] shouldBe thrownBy {Pipes.empty[Any].pickResources(ResourceSet.infinite) }
  }
}
