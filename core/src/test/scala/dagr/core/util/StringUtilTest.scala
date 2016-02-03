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

/**
  * Tests for StringUtil
  */
class StringUtilTest extends UnitSpec {
  "StringUtil.camelToGnu" should " correctly handle simple multi-word variable names" in {
    StringUtil.camelToGnu("fooBar") should be ("foo-bar")
    StringUtil.camelToGnu("oneTwoThreeFourFiveSixSeven") should be ("one-two-three-four-five-six-seven")
  }

  it should " not modify single word variable names" in {
    StringUtil.camelToGnu("foo") should be ("foo")
  }

  it should " replace initial caps but not insert a hyphen at the start of a word" in {
    StringUtil.camelToGnu("BigBad") should be ("big-bad")
  }

  it should " insert hyphens between adjacent capital letters" in {
    StringUtil.camelToGnu("TisASillyName") should be ("tis-a-silly-name")
  }

  it should " handle single letter names" in {
    StringUtil.camelToGnu("A") should be ("a")
    StringUtil.camelToGnu("b") should be ("b")
  }
}
