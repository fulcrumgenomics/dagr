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

import scala.util.{Success, Failure}


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

  // TODO: add moar tests
}
