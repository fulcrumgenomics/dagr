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
package dagr.commons.util

/**
  * Tests for StringUtil
  */
class StringUtilTest extends UnitSpec {

  "StringUtil.wrapString" should "wrap a string with a prefix and suffix" in {
    StringUtil.enclose("pre-", "body", "-suffix") shouldBe "pre-body-suffix"
  }

  "StringUtil.wordWrap" should "wrap an input string with newlines" in {
    StringUtil.wordWrap("a b bb b c", maxLength=1) shouldBe "a\nb\nbb\nb\nc"
  }

  "StringUtil.levenshteinDistance" should "compute the levenshtein distance between two strings" in {
    StringUtil.levenshteinDistance("abc", "abc",  swap=1,  substitution=1,  insertion=1,  deletion=1)  shouldBe 0
    StringUtil.levenshteinDistance("abc", "def",  swap=0,  substitution=0,  insertion=0,  deletion=0)  shouldBe 0
    
    // swap
    StringUtil.levenshteinDistance("abc", "acb",  swap=1,  substitution=0,  insertion=0,  deletion=0)  shouldBe 0 // can be achieved using substitutions at no penalty
    StringUtil.levenshteinDistance("abc", "acb",  swap=0,  substitution=0,  insertion=0,  deletion=0)  shouldBe 0
    StringUtil.levenshteinDistance("abc", "acb",  swap=1,  substitution=10, insertion=10, deletion=10) shouldBe 1

    // substitution
    StringUtil.levenshteinDistance("abc", "abd",  swap=0,  substitution=1,  insertion=0,  deletion=0)  shouldBe 0 // can be with an insertion then deletion using substitutions at no penalty
    StringUtil.levenshteinDistance("abc", "abd",  swap=0,  substitution=0,  insertion=0,  deletion=0)  shouldBe 0
    StringUtil.levenshteinDistance("abc", "abd",  swap=10, substitution=1,  insertion=10, deletion=10) shouldBe 1

    // deletion
    StringUtil.levenshteinDistance("abc", "ab",   swap=0,  substitution=0,  insertion=0,  deletion=1)  shouldBe 1 // deletions cannot be achieved with other operators
    StringUtil.levenshteinDistance("abc", "ab",   swap=0,  substitution=0,  insertion=0,  deletion=0)  shouldBe 0
    StringUtil.levenshteinDistance("abc", "ab",   swap=10, substitution=10, insertion=10, deletion=1)  shouldBe 1

    // insertion
    StringUtil.levenshteinDistance("abc", "abcd", swap=0,  substitution=0,  insertion=1,  deletion=0)  shouldBe 1 // insertions cannot be achieved with other operators
    StringUtil.levenshteinDistance("abc", "abcd", swap=0,  substitution=0,  insertion=0,  deletion=0)  shouldBe 0
    StringUtil.levenshteinDistance("abc", "abcd", swap=10, substitution=10, insertion=1,  deletion=10) shouldBe 1
  }

  "StringUtil.columnIt" should "behave like Unix's columnt -t" in {
    StringUtil.columnIt(rows=List(List("1,1", "1,2"), List("2,1", "2,2"))) shouldBe "1,1 1,2\n2,1 2,2"
    an[IllegalArgumentException] should be thrownBy StringUtil.columnIt(rows=List(List("1,1", "1,2"), List("2,1")))
  }

  "StringUtil.camelToGnu" should "correctly handle simple multi-word variable names" in {
    StringUtil.camelToGnu("fooBar") should be ("foo-bar")
    StringUtil.camelToGnu("oneTwoThreeFourFiveSixSeven") should be ("one-two-three-four-five-six-seven")
  }

  it should "not modify single word variable names" in {
    StringUtil.camelToGnu("foo") should be ("foo")
  }

  it should "replace initial caps but not insert a hyphen at the start of a word" in {
    StringUtil.camelToGnu("BigBad") should be ("big-bad")
  }

  it should "insert hyphens between adjacent capital letters" in {
    StringUtil.camelToGnu("TisASillyName") should be ("tis-a-silly-name")
  }

  it should "handle single letter names" in {
    StringUtil.camelToGnu("A") should be ("a")
    StringUtil.camelToGnu("b") should be ("b")
  }

  "StringUtil.addSpacesToCamelCase" should "correctly handle simple multi-word variable names" in {
    StringUtil.addSpacesToCamelCase("fooBar") should be ("foo Bar")
    StringUtil.addSpacesToCamelCase("oneTwoThreeFourFiveSixSeven") should be ("one Two Three Four Five Six Seven")
  }

  it should "not modify single word variable names" in {
    StringUtil.addSpacesToCamelCase("foo") should be ("foo")
  }

  it should "not add a space when the word starts with a caps" in {
    StringUtil.addSpacesToCamelCase("BigBad") should be ("Big Bad")
  }

  it should "insert spaces between adjacent capital letters" in {
    StringUtil.addSpacesToCamelCase("TisASillyName") should be ("Tis A Silly Name")
  }

  it should "handle single letter names" in {
    StringUtil.addSpacesToCamelCase("A") should be ("A")
    StringUtil.addSpacesToCamelCase("b") should be ("b")
  }

  it should "add a space between adjacen upper case characters" in {
    StringUtil.addSpacesToCamelCase("BBIAB") should be ("B B I A B")
  }
}
