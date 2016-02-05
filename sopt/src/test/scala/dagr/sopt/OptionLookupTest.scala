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

package dagr.sopt

import dagr.sopt.util.UnitSpec
import dagr.sopt.OptionLookup.{OptionType, OptionAndValues}

import scala.util.Try

/** Tests OptionLookup */
class OptionLookupTest extends UnitSpec {
  "OptionLookup.convertFlagValue" should "convert the flag value to a recognized value" in {
    OptionLookup.convertFlagValue("true").get  shouldBe "true"
    OptionLookup.convertFlagValue("TRUE").get  shouldBe "true"
    OptionLookup.convertFlagValue("T").get     shouldBe "true"
    OptionLookup.convertFlagValue("t").get     shouldBe "true"
    OptionLookup.convertFlagValue("False").get shouldBe "false"
    OptionLookup.convertFlagValue("FALSE").get shouldBe "false"
    OptionLookup.convertFlagValue("F").get     shouldBe "false"
    OptionLookup.convertFlagValue("f").get     shouldBe "false"
    OptionLookup.convertFlagValue("yes").get   shouldBe "true"
    OptionLookup.convertFlagValue("YES").get   shouldBe "true"
    OptionLookup.convertFlagValue("Y").get     shouldBe "true"
    OptionLookup.convertFlagValue("y").get     shouldBe "true"
    OptionLookup.convertFlagValue("No").get    shouldBe "false"
    OptionLookup.convertFlagValue("NO").get    shouldBe "false"
    OptionLookup.convertFlagValue("N").get     shouldBe "false"
    OptionLookup.convertFlagValue("n").get     shouldBe "false"
  }

  it should "throw an IllegalFlagValueException when trying convert an unrecognized flag value" in {
    OptionLookup.convertFlagValue("unknown").isFailure shouldBe true
  }

  "OptionLookup.OptionValues.add" should "set the value for a flag option type to true" in {
    val optionValues = new OptionAndValues(OptionType.Flag, Seq("name"))
    optionValues.add("name")
    optionValues.toList shouldBe List("true")
  }

  it should "add a single value for a flag option type" in {
    val optionValues = new OptionAndValues(OptionType.Flag, Seq("name"))
    optionValues.add("name", "true")
    optionValues.toList shouldBe List("true")
  }

  it should "return a failure with a TooManyValuesException when multiple values are given to flag option type" in {
    val optionValues = new OptionAndValues(OptionType.Flag, Seq("name"))
    val returnValue = optionValues.add("name", "true", "false")
    returnValue.isFailure shouldBe true
    returnValue.failed.get.getClass shouldBe classOf[TooManyValuesException]
  }

  it should "return a failure with a TooFewValuesException when a no values are given to a single value option type" in {
    val optionValues = new OptionAndValues(OptionType.SingleValue, Seq("name"))
    val returnValue = optionValues.add("name")
    returnValue.isFailure shouldBe true
    returnValue.failed.get.getClass shouldBe classOf[TooFewValuesException]
  }

  it should "add a single value for a single value option type" in {
    val optionValues = new OptionAndValues(OptionType.SingleValue, Seq("name"))
    optionValues.add("name", "true")
    optionValues.toList shouldBe List("true")
  }

  it should "return a failure with a TooManyValuesException when multiple values are given to single value option type" in {
    val optionValues = new OptionAndValues(OptionType.SingleValue, Seq("name"))
    val returValue = optionValues.add("name", "true", "false")
    returValue.isFailure shouldBe true
    returValue.failed.get.getClass shouldBe classOf[TooManyValuesException]
  }

  it should "return a failure with a TooFewValuesException when a no values are given to a mutiple value option type" in {
    val optionValues = new OptionAndValues(OptionType.MultiValue, Seq("name"))
    val returValue = optionValues.add("name")
    returValue.isFailure shouldBe true
    returValue.failed.get.getClass shouldBe classOf[TooFewValuesException]
  }

  it should "add a single value for a multi value option type" in {
    val optionValues = new OptionAndValues(OptionType.MultiValue, Seq("name"))
    optionValues.add("name", "true")
    optionValues.toList shouldBe List("true")
  }

  it should "add multiple values for a multi value option type" in {
    val optionValues = new OptionAndValues(OptionType.MultiValue, Seq("name"))
    optionValues.add("name", "true", "false")
    optionValues.toList shouldBe List("true", "false")
  }

  "OptionLookup.acceptsFlag" should "create a new argument value of type flag" in {
    Seq(
      List[String]("f"),
      List[String]("flag"),
      List[String]("f", "flag")
    ).foreach { names =>
      val optionLookup = new OptionLookup {}.acceptFlag(names: _*).get
      names.foreach { name =>
        optionLookup.optionMap should contain key name
        optionLookup.optionMap.get(name) shouldBe 'defined
        optionLookup.optionMap.get(name).get.toList shouldBe 'empty
        optionLookup.optionMap.get(name).get.optionNames should contain(name)
        optionLookup.optionMap.get(name).get.optionType shouldBe OptionType.Flag
      }
    }
  }

  "OptionLookup.acceptsSingleValue" should "create a new argument value of type single value" in {
    Stream(Stream[String]("s"), Stream[String]("single-argument"), Stream[String]("s", "single-argument")).foreach { names =>
      val optionLookup = new OptionLookup {}.acceptSingleValue(names: _*).get
      names.foreach { name =>
        optionLookup.optionMap should contain key name
        optionLookup.optionMap.get(name) shouldBe 'defined
        optionLookup.optionMap.get(name).get.toList shouldBe 'empty
        optionLookup.optionMap.get(name).get.optionNames should contain(name)
        optionLookup.optionMap.get(name).get.optionType shouldBe OptionType.SingleValue
      }
    }
  }

  "OptionLookup.acceptsMultipleValues" should "create a new argument value of type multiple value" in {
    Stream(Stream[String]("m"), Stream[String]("multi-argument"), Stream[String]("m", "multi-argument")).foreach { names =>
      val optionLookup = new OptionLookup {}.acceptMultipleValues(names: _*).get
      names.foreach { name =>
        optionLookup.optionMap should contain key name
        optionLookup.optionMap.get(name) shouldBe 'defined
        optionLookup.optionMap.get(name).get.toList shouldBe 'empty
        optionLookup.optionMap.get(name).get.optionNames should contain(name)
        optionLookup.optionMap.get(name).get.optionType shouldBe OptionType.MultiValue
      }
    }
  }

  "OptionLookup.accept" should "return a failure with a DuplicateOptionNameException if the same name is given twice for an option" in {
    val returnValue = new OptionLookup {} acceptFlag ("name1", "name2", "name1" )
    returnValue.isFailure shouldBe true
    returnValue.failed.get.getClass shouldBe classOf[DuplicateOptionNameException]
  }

  it should "return a failure with a DuplicateOptionNameException if the same name is given twice across two options" in {
    val returnValue = new OptionLookup {}.acceptFlag("name").get.acceptSingleValue("name")
    returnValue.isFailure shouldBe true
    returnValue.failed.get.getClass shouldBe classOf[DuplicateOptionNameException]
  }

  "OptionLookup.get" should "get options when using abbreviations for an option name" in {
    val name = "abcdefghijklmnopqrstuvwxyz"
    val optionLookup = new OptionLookup {}.acceptMultipleValues(name).get
    optionLookup.addOptionValues(name, s"$name-value")
    for (i <- 1 to name.length) {
      val prefix = name.substring(0, i)
      val optionValuesList = optionLookup.findExactOrPrefix(prefix)
      optionValuesList should have size 1
      optionValuesList.head.optionNames should contain (name)
    }
  }

  object OptionLookupWithPrefixes {
    val name1 = "012345"
    val name2 = "012ABC"
    val name3 = "nocommonprefix"
    val optionLookup = new OptionLookup {}.acceptMultipleValues(name1).get.acceptMultipleValues(name2).get.acceptMultipleValues(name3).get
    Seq(name1, name2, name3).foreach(name => optionLookup.addOptionValues(name, s"$name-value"))
  }

  it should "get options when using abbreviations for option names" in {
    import OptionLookupWithPrefixes._
    for (i <- 1 to name1.length) {
      val prefix = name1.substring(0, i)
      val optionValuesList = optionLookup.findExactOrPrefix(prefix)
      val names = Seq(name1, name2, name3).filter(_.startsWith(prefix))
      optionValuesList should have size names.size
      names.foreach(name =>
        optionValuesList.find(optionValue => optionValue.optionNames.contains(name)).size shouldBe 1
      )
    }
    optionLookup.findExactOrPrefix("thisisnotanoption") shouldBe Nil
  }

  "OptionLookup.hasOptionName" should "be true when there is a uniquely identifiably option" in {
    import OptionLookupWithPrefixes._
    Seq(name1, name2, name3).foreach(name => optionLookup.hasOptionName(name) shouldBe true)
    optionLookup.hasOptionName("no") shouldBe true // prefix
    optionLookup.hasOptionName("0123") shouldBe true // prefix
    optionLookup.hasOptionName("012A") shouldBe true // prefix
    optionLookup.hasOptionName("thisisnotanoption") shouldBe false
    optionLookup.hasOptionName("012") shouldBe false // multiple options with the given prefix
    optionLookup.hasOptionName("") shouldBe false // all options with the given prefix
  }

  "OptionLookup.hasOptionValues" should "be true if `getOptionValues` were to return at least one value" in {
    new OptionLookup {}.acceptMultipleValues("name").foreach { optionLookup =>
      optionLookup.hasOptionValues("name") shouldBe false
      optionLookup.addOptionValues("name", "value")
      optionLookup.hasOptionValues("name") shouldBe true
      optionLookup.addOptionValues("name", "value")
      optionLookup.hasOptionValues("name") shouldBe true
    }
  }

  "OptionLookup.getSingleValues" should "return the single value for the option with the given name or prefix, or a failure." in {
    new OptionLookup {}.acceptMultipleValues("name").foreach { optionLookup =>
      // empty
      var value: Try[String] = optionLookup.getSingleValues("name")
      value.isFailure shouldBe true
      value.failed.get.getClass shouldBe classOf[IllegalOptionNameException]
      // one element
      optionLookup.addOptionValues("name", "value")
      value = optionLookup.getSingleValues("name")
      value.isSuccess shouldBe true
      value.get shouldBe "value"
      // two elements
      optionLookup.addOptionValues("name", "proposition")
      value = optionLookup.getSingleValues("name")
      value.isFailure shouldBe true
      value.failed.get.getClass shouldBe classOf[IllegalOptionNameException]
    }
  }

  "OptionLookup.getOptionValues" should "get options that exist including prefixes of option names" in {
    import OptionLookupWithPrefixes._
    Seq(name1, name2, name3).foreach(name => optionLookup.getOptionValues(name).get shouldBe List(s"$name-value"))
    optionLookup.getOptionValues("0123").get shouldBe List(s"$name1-value") // prefix
    optionLookup.getOptionValues("012A").get shouldBe List(s"$name2-value") // prefix
    optionLookup.getOptionValues("no").get shouldBe List(s"$name3-value") // prefix
  }

  it should "return a failure with a IllegalOptionNameException when no values are found for the option name" in {
    import OptionLookupWithPrefixes._
    val returnValue = optionLookup.getOptionValues("thisisnotanoption")
    returnValue.isFailure shouldBe true
    returnValue.failed.get.getClass shouldBe classOf[IllegalOptionNameException]
  }

  it should "return a failure with a DuplicateOptionNameException when multiple options with that name exist" in {
    import OptionLookupWithPrefixes._
    var returnValue = optionLookup.getOptionValues("012") // multiple options with the given prefix
    returnValue.isFailure shouldBe true
    returnValue.failed.get.getClass shouldBe classOf[DuplicateOptionNameException]
    returnValue = optionLookup.getOptionValues("") // all options with the given prefix
    returnValue.isFailure shouldBe true
    returnValue.failed.get.getClass shouldBe classOf[DuplicateOptionNameException]
  }

  "OptionLookup.addOptionValues" should "add values to options including prefixes of option names" in {
    // NB: do not import OptionLookupWithPrefixes since we change its internal state with successful calls to addOptionValues
    val name1 = "012345"
    val name2 = "012ABC"
    val name3 = "nocommonprefix"
    val optionLookup = new OptionLookup {}.acceptMultipleValues(name1).get.acceptMultipleValues(name2).get.acceptMultipleValues(name3).get
    Seq(name1, name2, name3).foreach(name => optionLookup.addOptionValues(name, s"$name-value"))
    optionLookup.addOptionValues("0123", "1").get shouldBe List(s"$name1-value", "1") // prefix
    optionLookup.addOptionValues("012A", "1").get shouldBe List(s"$name2-value", "1") // prefix
    optionLookup.addOptionValues("no", "1").get shouldBe List(s"$name3-value", "1") // prefix
  }

  it should "return a failure with a IllegalOptionNameException when no options with that name exist" in {
    import OptionLookupWithPrefixes._
    val returnValue = optionLookup.addOptionValues("thisisnotanoption")
    returnValue.isFailure shouldBe true
    returnValue.failed.get.getClass shouldBe classOf[IllegalOptionNameException]
  }

  it should "return a failure with a DuplicateOptionNameException when multiple options with that name exist" in {
    import OptionLookupWithPrefixes._
    var returnValue = optionLookup.addOptionValues("012") // multiple options with the given prefix
    returnValue.isFailure shouldBe true
    returnValue.failed.get.getClass shouldBe classOf[DuplicateOptionNameException]
    returnValue = optionLookup.addOptionValues("") // all options with the given prefix
    returnValue.isFailure shouldBe true
    returnValue.failed.get.getClass shouldBe classOf[DuplicateOptionNameException]
  }

  "OptionLookup.OptionAndValues.add" should "return a failure with a IllegalOptionNameException when none if its names match the given name" in {
    val optionAndValues = new OptionAndValues(optionType=OptionType.Flag, Seq("some", "names", "go", "here"))
    val result = optionAndValues.add("not a name", "value")
    result.isFailure shouldBe true
    result.failed.get.getClass shouldBe classOf[IllegalOptionNameException]
  }

  "OptionLookup.OptionAndValues.isEmpty" should "should return true if no values have been added, false otherwise" in {
    val optionAndValues = new OptionAndValues(optionType=OptionType.SingleValue, Seq("name"))
    optionAndValues.isEmpty shouldBe true
    optionAndValues.add("name", "value")
    optionAndValues.isEmpty shouldBe false
  }

  "OptionLookup.printUnknown" should "throw an IllegalStateException when a name matches exactly" in {
    import OptionLookupWithPrefixes._
    an[IllegalStateException] should be thrownBy optionLookup.printUnknown(name1)
  }

  it should "return an empty string floor when the match is beyond the edit distance" in {
    import OptionLookupWithPrefixes._
    optionLookup.printUnknown(name1 + "abcdefghijklmnopqrstuvwxyz") shouldBe ""
  }

  it should "the next-best option name when within the edit distance" in {
    val name1 = "name1"
    var optionLookup = new OptionLookup {}.acceptMultipleValues(name1).get
    optionLookup.printUnknown("name3").indexOf(name1) should be > 0
    val name2 = "name2"
    optionLookup = optionLookup.acceptMultipleValues(name2).get
    optionLookup.printUnknown("name3").indexOf(name1) should be > 0
    optionLookup.printUnknown("name3").indexOf(name2) should be > 0
  }
}
