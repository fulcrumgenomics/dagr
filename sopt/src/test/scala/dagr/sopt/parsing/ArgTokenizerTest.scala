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

package dagr.sopt.parsing

import java.nio.file.Files

import dagr.commons.util.UnitSpec

import scala.util.{Failure, Success}

class ArgTokenizerTest extends UnitSpec {
  import ArgTokenizer._

  /** Creates a temporary directory, writes the lines to it, then returns the path as a String. */
  private def writeTmpArgFile(lines: Seq[String]) : String = {
    import scala.collection.JavaConversions.asJavaIterable
    val tmp = Files.createTempFile("args.", ".txt")
    Files.write(tmp, lines)
    tmp.toFile.deleteOnExit()
    tmp.toAbsolutePath.toString
  }

  "ArgTokenizer" should "iterate nothing if given no args" in {
    new ArgTokenizer().hasNext() shouldBe false
    an[NoSuchElementException] should be thrownBy new ArgTokenizer().next()
  }

  it should "return an [[EmptyArgumentException]] if an empty string is given" in {
    val tryVal = new ArgTokenizer("").next()
    tryVal shouldBe 'failure
    an[OptionNameException] should be thrownBy (throw tryVal.failed.get)
  }

  it should "tokenize a short option: '-f'" in {
    val tokenizer = new ArgTokenizer("-f")
    tokenizer.next().get shouldBe ArgOption(name="f")
    tokenizer.hasNext() shouldBe false
  }

  it should "tokenize a short option: '-f value'" in {
    val tokenizer = new ArgTokenizer("-f", "value")
    tokenizer.next().get shouldBe ArgOption(name = "f")
    tokenizer.next().get shouldBe ArgValue(value = "value")
    tokenizer.hasNext() shouldBe false
  }

  it should "tokenize a short option: '-fvalue'" in {
    val tokenizer = new ArgTokenizer("-fvalue")
    tokenizer.next().get shouldBe ArgOptionAndValue(name = "f", value = "value")
    tokenizer.hasNext() shouldBe false
  }

  it should "tokenize a short option: '-f=value'" in {
    val tokenizer = new ArgTokenizer("-f=value")
    tokenizer.next().get shouldBe ArgOptionAndValue(name = "f", value = "value")
    tokenizer.hasNext() shouldBe false
  }

  it should "tokenize a short option: '-fvalue value'" in {
    val tokenizer = new ArgTokenizer("-f=value", "value")
    tokenizer.next().get shouldBe ArgOptionAndValue(name = "f", value = "value")
    tokenizer.next().get shouldBe ArgValue(value="value")
    tokenizer.hasNext() shouldBe false
  }

  it should "tokenize a short option: '-f value value'" in {
    val tokenizer = new ArgTokenizer("-f", "value", "value")
    tokenizer.next().get shouldBe ArgOption(name = "f")
    tokenizer.next().get shouldBe ArgValue(value="value")
    tokenizer.next().get shouldBe ArgValue(value="value")
    tokenizer.hasNext() shouldBe false
  }

  it should "tokenize a short option: '-f=value value'" in {
    val tokenizer = new ArgTokenizer("-f=value", "value")
    tokenizer.next().get shouldBe ArgOptionAndValue(name = "f", value = "value")
    tokenizer.next().get shouldBe ArgValue(value="value")
    tokenizer.hasNext() shouldBe false
  }

  it should "tokenize a long option: '--long'" in {
    val tokenizer = new ArgTokenizer("--long")
    tokenizer.next().get shouldBe ArgOption(name = "long")
    tokenizer.hasNext() shouldBe false
  }

  it should "tokenize a long option: '--long value'" in {
    val tokenizer = new ArgTokenizer("--long", "value")
    tokenizer.next().get shouldBe ArgOption(name = "long")
    tokenizer.next().get shouldBe ArgValue(value = "value")
    tokenizer.hasNext() shouldBe false
  }

  it should "not tokenize a long option: '--longvalue'" in {
    val tokenizer = new ArgTokenizer("--longvalue")
    tokenizer.next().get shouldBe ArgOption(name = "longvalue")
    tokenizer.hasNext() shouldBe false
  }

  it should "tokenize a long option: '--long=value'" in {
    val tokenizer = new ArgTokenizer("--long=value")
    tokenizer.next().get shouldBe ArgOptionAndValue(name = "long", value="value")
    tokenizer.hasNext() shouldBe false
  }

  it should "tokenize a long option: '--long=v'" in {
    val tokenizer = new ArgTokenizer("--long=v")
    tokenizer.next().get shouldBe ArgOptionAndValue(name = "long", value="v")
    tokenizer.hasNext() shouldBe false
  }

  it should "tokenize a long option: '--long value value'" in {
    val tokenizer = new ArgTokenizer("--long", "value", "value")
    tokenizer.next().get shouldBe ArgOption(name = "long")
    tokenizer.next().get shouldBe ArgValue(value = "value")
    tokenizer.next().get shouldBe ArgValue(value = "value")
    tokenizer.hasNext() shouldBe false
  }

  it should "tokenize a long option: '--long=value value'" in {
    val tokenizer = new ArgTokenizer("--long=value", "value")
    tokenizer.next().get shouldBe ArgOptionAndValue(name = "long", value="value")
    tokenizer.next().get shouldBe ArgValue(value = "value")
    tokenizer.hasNext() shouldBe false
  }

  it should "return an ArgValue when missing a leading dash" in {
    val argList = List(
      ("fvalue", "value"),
      ("f=value", "value"),
      ("long=value", "value")
    )
    argList.foreach { args =>
      val tokenizer = new ArgTokenizer(args._1, args._2)
      tokenizer.next().get shouldBe ArgValue(value=args._1)
      tokenizer.next().get shouldBe ArgValue(value=args._2)
      tokenizer.hasNext() shouldBe false
    }
  }

  it should "throw an [[OptionNameException]] when missing characters a leading dash(es)" in {
    val argList = List(
      List("-"),
      List("-", "value")
    )
    argList.foreach { args =>
      val tokenizer = new ArgTokenizer(args:_*)
      val tryVal = tokenizer.next()
      tryVal shouldBe 'failure
      an[OptionNameException] should be thrownBy (throw tryVal.failed.get)
    }
  }

  it should "throw an [[OptionNameException]] when a long option ends with an equals ('--long=')" in {
    val tokenizer = new ArgTokenizer("--long=")
    val tryVal = tokenizer.next()
    tryVal shouldBe 'failure
    an[OptionNameException] should be thrownBy (throw tryVal.failed.get)
  }

  it should "tokenize a up until '--' is found" in {
    val tokenizer = new ArgTokenizer("--long=value", "value", "--", "value")
    tokenizer.next().get shouldBe ArgOptionAndValue(name="long", value="value")
    tokenizer.next().get shouldBe ArgValue(value = "value")
    tokenizer.hasNext() shouldBe false
    tokenizer.takeRemaining shouldBe List("value")
  }

  it should "tokenize '---foo' into the option '-foo'" in {
    val tokenizer = new ArgTokenizer("---foo")
    tokenizer.next().get shouldBe ArgOption(name="-foo") // FIXME
  }

  it should "tokenize '-@' into the option '@'" in {
    val tokenizer = new ArgTokenizer("-@")
    tokenizer.next().get shouldBe ArgOption(name="@") // FIXME
  }

  it should "tokenize '-f-1' into the option and value 'f' and '-1'" in {
    val tokenizer = new ArgTokenizer("-f-1")
    tokenizer.next().get shouldBe ArgOptionAndValue(name="f", value="-1") // FIXME
  }

  it should "substitute values from a file" in {
    val path = writeTmpArgFile(Seq("--foo=oof", "--bar", "1 2 3"))
    val tokenizer = new ArgTokenizer(Seq("--hello", "@" + path, "--good=bye"), Some("@"))
    tokenizer.next() shouldBe Success(ArgOption(name="hello"))
    tokenizer.next() shouldBe Success(ArgOptionAndValue(name="foo", value="oof"))
    tokenizer.next() shouldBe Success(ArgOption(name="bar"))
    tokenizer.next() shouldBe Success(ArgValue(value="1 2 3"))
    tokenizer.next() shouldBe Success(ArgOptionAndValue(name="good", value="bye"))
  }

  it should "handle an empty arguments file" in {
    val path = writeTmpArgFile(Seq())
    val tokenizer = new ArgTokenizer(Seq("--hello", "@" + path, "--good=bye"), Some("@"))
    tokenizer.next() shouldBe Success(ArgOption(name="hello"))
    tokenizer.next() shouldBe Success(ArgOptionAndValue(name="good", value="bye"))
  }

  it should "handle an arguments file with empty and whitespace-only lines" in {
    val path = writeTmpArgFile(Seq("--foo", "", "  ", "--bar"))
    val tokenizer = new ArgTokenizer(Seq("--hello", "@" + path, "--good=bye"), Some("@"))
    tokenizer.next() shouldBe Success(ArgOption(name="hello"))
    tokenizer.next() shouldBe Success(ArgOption(name="foo"))
    tokenizer.next() shouldBe Success(ArgOption(name="bar"))
    tokenizer.next() shouldBe Success(ArgOptionAndValue(name="good", value="bye"))
  }

  it should "trim leading and trailing whitespace from argument file lines" in {
    val path = writeTmpArgFile(Seq("  --foo", "--bar ", " --whee "))
    val tokenizer = new ArgTokenizer(Seq("--hello", "@" + path, "--good=bye"), Some("@"))
    tokenizer.next() shouldBe Success(ArgOption(name="hello"))
    tokenizer.next() shouldBe Success(ArgOption(name="foo"))
    tokenizer.next() shouldBe Success(ArgOption(name="bar"))
    tokenizer.next() shouldBe Success(ArgOption(name="whee"))
    tokenizer.next() shouldBe Success(ArgOptionAndValue(name="good", value="bye"))
  }

  it should "recursive parse arg files" in {
    val path1 = writeTmpArgFile(Seq("--three"))
    val path2 = writeTmpArgFile(Seq("--two", "@" + path1, "--four"))
    val path3 = writeTmpArgFile(Seq("--one", "@" + path2, "--five"))

    val tokenizer = new ArgTokenizer(Seq("--zero", "@" + path3, "--six"), Some("@"))
    tokenizer.next() shouldBe Success(ArgOption(name="zero"))
    tokenizer.next() shouldBe Success(ArgOption(name="one"))
    tokenizer.next() shouldBe Success(ArgOption(name="two"))
    tokenizer.next() shouldBe Success(ArgOption(name="three"))
    tokenizer.next() shouldBe Success(ArgOption(name="four"))
    tokenizer.next() shouldBe Success(ArgOption(name="five"))
    tokenizer.next() shouldBe Success(ArgOption(name="six"))
  }

  it should "return a Failure when hitting an argment file that doesn't exist" in {
    val path = "/path/to/nowhere/I/mean_it/12345.txt"

    val tokenizer = new ArgTokenizer(Seq("--one", "--two=three", "-f", "@" + path, "--six"), Some("@"))
    tokenizer.next() shouldBe Success(ArgOption(name="one"))
    tokenizer.next() shouldBe Success(ArgOptionAndValue(name="two", value="three"))
    tokenizer.next() shouldBe Success(ArgOption(name="f"))
    tokenizer.next() shouldBe an[Failure[_]]
    tokenizer.takeRemaining shouldBe Seq("--six")
  }

}
