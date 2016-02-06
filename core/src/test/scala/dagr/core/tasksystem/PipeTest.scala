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

import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.core.util.UnitSpec

/**
  * Tests for the piping together of tasks
  */
class PipeTest extends UnitSpec {
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
  case class Cat(val f: String) extends PipeOut[Text] with TestResources { def args = "cat" :: f.toString :: Nil }
  case class MakeCsv()  extends Pipe[Text,Csv] with TestResources { def args = "sed" :: "s/ +/,/g" :: Nil }
  case class CsvToTsv() extends Pipe[Csv,Tsv] with TestResources  { def args = "tr" :: "','" :: """'\t'""" :: Nil }
  case class Column()   extends Pipe[Text,Text] with TestResources { def args = "column" :: "-t" :: Nil }
  case class BgZip()    extends Pipe[Text,Binary] with VariableResources {
    override def pickResources(available: ResourceSet): Option[ResourceSet] = available.subset(Cores(1), Cores(8), Memory(bytes))
    override def args: Seq[Any] = "bgzip" :: "--stdout" :: "--threads" :: resources.cores ::  Nil
  }
  case class BgUnzip()  extends Pipe[Binary,Text] with TestResources { def args = "bgzip" :: "--decompress" :: "--stdout" :: Nil  }

  "Pipe" should "connect some simple tasks" in {
    val pipe = Cat("foo.txt") | MakeCsv()
    pipe.args.length shouldBe 5
    pipe.args.mkString(" ") shouldBe "cat foo.txt | sed s/ +/,/g"

    val pipe2 = Cat("bar.txt") | MakeCsv() | CsvToTsv() | Column()
    pipe2.args.mkString(" ") shouldBe """cat bar.txt | sed s/ +/,/g | tr ',' '\t' | column -t"""
  }

  it should "handle re-use of parts of pipes" in {
    val start = Cat("foo.txt") | MakeCsv()
    val pipe1 = start | CsvToTsv()
    val pipe2 = start | Column()
    pipe1.args.mkString(" ") shouldBe """cat foo.txt | sed s/ +/,/g | tr ',' '\t'"""
    pipe2.args.mkString(" ") shouldBe """cat foo.txt | sed s/ +/,/g | column -t"""
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
    val pipe = Cat("stuff.csv") | MakeCsv() | CsvToTsv() | BgZip()
    pipe.pickResources(ResourceSet.infinite) shouldBe Some(ResourceSet(3*cores + 8, 4*bytes))
    pipe.pickResources(ResourceSet.empty)    shouldBe None
    pipe.pickResources(ResourceSet(5, Long.MaxValue)) shouldBe Some(ResourceSet(4.75, 4*bytes))
  }

  it should "throw an exception when constructing a pipe chain with multiple variable resource tasks" in {
    val start = Cat("stuff.csv") | MakeCsv() | CsvToTsv() | BgZip() | BgUnzip()
    an[IllegalArgumentException] shouldBe thrownBy { start | BgZip() }
  }

  it should "allow use of Pipe.empty anywhere in a pipeline" in {
    List(true, false).foreach(col => {
      val maybeColumn = if (col) new Column() else Pipes.empty[Text]
      val pipe = Cat("foo.txt") | maybeColumn | MakeCsv()
      pipe.args.mkString(" ") shouldBe { if (col) "cat foo.txt | column -t | sed s/ +/,/g" else "cat foo.txt | sed s/ +/,/g" }
    })

    val silly = Pipes.empty[Csv] | Pipes.empty[Csv] | Pipes.empty[Text]
    val pipe = Cat("foo.txt") | MakeCsv() | silly | Column()
    pipe.args.mkString(" ") shouldBe """cat foo.txt | sed s/ +/,/g | column -t"""
  }

  it should "compile with a subtype as input in a pipe" in {
    val pipe1 = Cat("foo.txt") | BgZip() // exact type: Text
    val pipe2 = Cat("foo.txt") | MakeCsv() | BgZip() // subtype: Csv
    val pipe3 = Cat("foo.txt") | MakeCsv() | CsvToTsv() | BgZip() // subtype: Tsv
  }
}
