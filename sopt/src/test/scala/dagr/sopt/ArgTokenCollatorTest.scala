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
import dagr.sopt.ArgTokenizer.{ArgOptionAndValue, ArgValue, Token, ArgOption}

import scala.util.{Try, Success, Failure}


class ArgTokenCollatorTest extends UnitSpec {

  "ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue" should "match an arg value" in {
    ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue(tryToken = Success(ArgValue("value")), "name") shouldBe true
    ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue(tryToken = Success(ArgValue("value")), "") shouldBe true
  }

  it should "match an arg option and value" in {
    ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue(tryToken = Success(ArgOptionAndValue("name", "value")), "name") shouldBe true
    ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue(tryToken = Success(ArgOptionAndValue("foo", "value")), "foo") shouldBe true
  }

  it should "not match an arg option and value when the names are different" in {
    ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue(tryToken = Success(ArgOptionAndValue("name1", "value")), "name")
    ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue(tryToken = Success(ArgOptionAndValue("foo1", "value")), "foo")
  }

  it should "not match if a failure is given" in {
    ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue(tryToken = Failure(new Exception), "name")
    ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue(tryToken = Failure(new Exception), "foo")
  }

  it should "not match if an ArgOption is given" in {
    ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue(tryToken = Success(ArgOption("name")), "name")
  }

  "ArgTokenCollator.hasNext" should "should return true when starting with an ArgValue but the returned value should be a failure" in {
    val tokenizer = new ArgTokenizer("value")
    val collator = new ArgTokenCollator(tokenizer)
    collator.hasNext shouldBe true
    collator.isEmpty shouldBe false
    val n = collator.next
    n.isFailure shouldBe true
    n.failed.get.getClass shouldBe classOf[OptionNameException]
  }

  "ArgTokenCollator.advance" should "should group values for the same long option" in {
    val tokenizer = new ArgTokenizer("--value", "value", "--value", "value", "value")
    val collator = new ArgTokenCollator(tokenizer)
    val tokens = collator.toSeq
    tokens.size shouldBe 2
    // token 1
    var tokenTry = tokens.head
    tokenTry.isSuccess shouldBe true
    var token = tokenTry.get
    token.name shouldBe "value"
    token.values shouldBe Seq("value")
    // token 2
    tokenTry = tokens.last
    tokenTry.isSuccess shouldBe true
    token = tokenTry.get
    token.name shouldBe "value"
    token.values shouldBe Seq("value", "value")
  }

  it should "should group values for the same short (flag) option" in {
    val tokenizer = new ArgTokenizer("-vvalue", "-vvalue", "value")
    val collator = new ArgTokenCollator(tokenizer)
    val tokens = collator.toSeq
    tokens.size shouldBe 1
    // token 1
    var tokenTry = tokens.head
    tokenTry.isSuccess shouldBe true
    val token = tokenTry.get
    token.name shouldBe "v"
    token.values shouldBe Seq("value", "value", "value")
  }

  "ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue" should "return true for args with the same name, false otherwise" in {
    ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue(Success(ArgValue("value")), "opt") shouldBe true
    ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue(Success(ArgOptionAndValue("opt", "value")), "opt") shouldBe true
    ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue(Success(ArgOptionAndValue("opt2", "value")), "opt") shouldBe false
    ArgTokenCollator.isArgValueOrSameNameArgOptionAndValue(Success(ArgOption("opt")), "opt") shouldBe false
  }
}
