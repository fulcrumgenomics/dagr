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

package dagr.core.exec

import dagr.core.UnitSpec

class ResourceTest extends UnitSpec {

  "Resource.parseSizeToBytes" should "parse a string representing byte values" in {
    Resource.parseSizeToBytes("12345") shouldBe BigInt(12345)
    Resource.parseSizeToBytes("Not a number") shouldBe BigInt(-1)

    Resource.parseSizeToBytes("1k") shouldBe BigInt(1024)
    Resource.parseSizeToBytes("1K") shouldBe BigInt(1024)
    Resource.parseSizeToBytes("1kb") shouldBe BigInt(1024)
    Resource.parseSizeToBytes("1Kb") shouldBe BigInt(1024)

    Resource.parseSizeToBytes("1m") shouldBe BigInt(1024)*BigInt(1024)
    Resource.parseSizeToBytes("1M") shouldBe BigInt(1024)*BigInt(1024)
    Resource.parseSizeToBytes("1mb") shouldBe BigInt(1024)*BigInt(1024)
    Resource.parseSizeToBytes("1Mb") shouldBe BigInt(1024)*BigInt(1024)

    Resource.parseSizeToBytes("1g") shouldBe BigInt(1024)*BigInt(1024)*BigInt(1024)
    Resource.parseSizeToBytes("1G") shouldBe BigInt(1024)*BigInt(1024)*BigInt(1024)
    Resource.parseSizeToBytes("1gb") shouldBe BigInt(1024)*BigInt(1024)*BigInt(1024)
    Resource.parseSizeToBytes("1Gb") shouldBe BigInt(1024)*BigInt(1024)*BigInt(1024)

    Resource.parseSizeToBytes("1t") shouldBe BigInt(1024)*BigInt(1024)*BigInt(1024)*BigInt(1024)
    Resource.parseSizeToBytes("1T") shouldBe BigInt(1024)*BigInt(1024)*BigInt(1024)*BigInt(1024)
    Resource.parseSizeToBytes("1tb") shouldBe BigInt(1024)*BigInt(1024)*BigInt(1024)*BigInt(1024)
    Resource.parseSizeToBytes("1Tb") shouldBe BigInt(1024)*BigInt(1024)*BigInt(1024)*BigInt(1024)

    Resource.parseSizeToBytes("1p") shouldBe BigInt(1024)*BigInt(1024)*BigInt(1024)*BigInt(1024)*BigInt(1024)
    Resource.parseSizeToBytes("1P") shouldBe BigInt(1024)*BigInt(1024)*BigInt(1024)*BigInt(1024)*BigInt(1024)
    Resource.parseSizeToBytes("1pb") shouldBe BigInt(1024)*BigInt(1024)*BigInt(1024)*BigInt(1024)*BigInt(1024)
    Resource.parseSizeToBytes("1Pb") shouldBe BigInt(1024)*BigInt(1024)*BigInt(1024)*BigInt(1024)*BigInt(1024)
  }

  "Resource.parseBytesToSize" should "return the number of bytes as a string" in {
    Resource.parseBytesToSize(209*1024) shouldBe "0.2m"
    Resource.parseBytesToSize(301*1024) shouldBe "0.29m"
    Resource.parseBytesToSize(511*1024) shouldBe "0.5m"
    Resource.parseBytesToSize(512*1024) shouldBe "0.5m"
    Resource.parseBytesToSize(1024*1024) shouldBe "1m"
    Resource.parseBytesToSize(512*1024*1024) shouldBe "512m"
    Resource.parseBytesToSize(1024*1024*1024) shouldBe "1024m"
    Resource.parseBytesToSize((1024+1)*1024*1024) shouldBe "1025m"
    Resource.parseBytesToSize((1024+512)*1024*1024) shouldBe "1536m"
  }

  "Resource" should "add, subtract, and compare resources" in {
    val low = Memory.none
    val mid = Memory(10)
    val high = Memory(20)

    // subtract
    (mid - low) shouldBe Memory(10)
    (high - mid) shouldBe Memory(10)
    // add
    (mid + low) shouldBe Memory(10)
    (high + mid) shouldBe Memory(30)
    // <
    (low < mid) shouldBe true
    (mid < low) shouldBe false
    (low < low) shouldBe false
    (mid < high) shouldBe true
    (high < mid) shouldBe false
    // <=
    (low <= mid) shouldBe true
    (mid <= low) shouldBe false
    (low <= low) shouldBe true
    (mid <= high) shouldBe true
    (high <= mid) shouldBe false
    // >
    (low > mid) shouldBe false
    (mid > low) shouldBe true
    (low > low) shouldBe false
    (mid > high) shouldBe false
    (high > mid) shouldBe true
    // >=
    (low >= mid) shouldBe false
    (mid >= low) shouldBe true
    (low >= low) shouldBe true
    (mid >= high) shouldBe false
    (high >= mid) shouldBe true
  }

  it should "not allow negative values" in {
    an[IllegalArgumentException] shouldBe thrownBy { Cores(-1) }
    an[IllegalArgumentException] shouldBe thrownBy { Cores(2) - Cores(4) }
    an[IllegalArgumentException] shouldBe thrownBy { Memory(-1) }
    an[IllegalArgumentException] shouldBe thrownBy { Memory(2) - Memory(4) }
    an[IllegalArgumentException] shouldBe thrownBy { Memory("512M") - Memory("1g") }
  }

  it should "always give back the numeric value via toString for easy arg usage" in {
    Cores(7).toString shouldBe "7.0"
    Cores.none.toString shouldBe "0.0"
    Cores(5000).toString shouldBe "5000.0"
    Memory(1024).toString shouldBe "1024"
    Memory("1G").toString shouldBe (1024*1024*1024).toString
  }

  "Memory" should "represent its value in pretty formats" in {
    var mem = Memory(1024.toLong * 1024.toLong * 1024.toLong * 2.toLong)
    mem.kb shouldBe "2097152k"
    mem.mb shouldBe "2048m"
    mem.gb shouldBe "2g"
    mem.prettyString shouldBe "2g"
    mem = Memory(1024.toLong * 1024.toLong * 2.toLong)
    mem.prettyString shouldBe "2m"
    mem = Memory(1024.toLong * 2.toLong)
    mem.prettyString shouldBe "2k"
  }
}
