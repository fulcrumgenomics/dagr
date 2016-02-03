package dagr.sopt

import dagr.sopt.util.UnitSpec

class ArgTokenizerTest extends UnitSpec {
  import ArgTokenizer._

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
}