/*
 * The MIT License
 *
 * Copyright (c) 2016 Fulcrum Genomics LLC
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
 *
 */

package dagr.sopt.cmdline

import java.util

import dagr.commons.util.UnitSpec
import dagr.sopt.util.TermCode
import org.scalatest.BeforeAndAfterAll

/**
  * Tests for ClpArgumentDefinitionPrinting.
  */
class ClpArgumentDefinitionPrintingTest extends UnitSpec with BeforeAndAfterAll {

  import ClpArgumentDefinitionPrinting.makeDefaultValueString

  private var printColor = true

  override protected def beforeAll(): Unit = {
    printColor = TermCode.printColor
    TermCode.printColor = false
  }

  override protected def afterAll(): Unit = {
    TermCode.printColor = printColor
  }

  "ClpArgumentDefinitionPrinting.makeDefaultValueString" should "print the default value" in {
    makeDefaultValueString(None) shouldBe ""
    makeDefaultValueString(Some(None)) shouldBe ""
    makeDefaultValueString(Some(Nil)) shouldBe ""
    makeDefaultValueString(Some(Set.empty)) shouldBe ""
    makeDefaultValueString(Some(new util.ArrayList[java.lang.Integer]())) shouldBe ""
    makeDefaultValueString(Some(Some("Value"))) shouldBe "[Default: Value]. "
    makeDefaultValueString(Some("Value")) shouldBe "[Default: Value]. "
    makeDefaultValueString(Some(Some(Some("Value")))) shouldBe "[Default: Some(Value)]. "
    makeDefaultValueString(Some(List("A", "B", "C"))) shouldBe "[Default: A, B, C]. "
  }

  private def printArgumentUsage(name: String, shortName: String, theType: String,
                                 collectionDescription: Option[String], argumentDescription: String): String = {
    val stringBuilder = new StringBuilder
    ClpArgumentDefinitionPrinting.printArgumentUsage(stringBuilder=stringBuilder, name, shortName, theType, collectionDescription, argumentDescription)
    stringBuilder.toString
  }

  // NB: does not test column wrapping
  "ClpArgumentDefinitionPrinting.printArgumentUsage" should "print usages" in {
    val longName    = "long-name"
    val shortName   = "s"
    val theType     = "TheType"
    val description = "Some description"

    printArgumentUsage(longName, shortName, theType, None, description).startsWith(s"-$shortName $theType, --$longName=$theType") shouldBe true
    printArgumentUsage(longName, shortName, "Boolean", None, description).startsWith(s"-$shortName [true|false], --$longName[=true|false]") shouldBe true
    printArgumentUsage(longName, "", theType, None, description).startsWith(s"--$longName=$theType") shouldBe true
    printArgumentUsage(longName, shortName, theType, Some("+"), description).startsWith(s"-$shortName $theType+, --$longName=$theType+") shouldBe true
    printArgumentUsage(longName, shortName, theType, Some("{0,20}"), description).startsWith(s"-$shortName $theType{0,20}, --$longName=$theType{0,20}") shouldBe true
  }
}
