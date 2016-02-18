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
  * Tests for TimeUtil
  */
class TimeUtilTest extends UnitSpec {
  "TimeUtil.formatElapsedTime" should "format the elapsed time in DD:HH:MM:SS" in {
    TimeUtil.formatElapsedTime(0)      shouldBe "00:00:00"
    TimeUtil.formatElapsedTime(1)      shouldBe "00:00:01"
    TimeUtil.formatElapsedTime(11)     shouldBe "00:00:11"
    TimeUtil.formatElapsedTime(60)     shouldBe "00:01:00"
    TimeUtil.formatElapsedTime(71)     shouldBe "00:01:11"
    TimeUtil.formatElapsedTime(671)    shouldBe "00:11:11"
    TimeUtil.formatElapsedTime(4271)   shouldBe "01:11:11"
    TimeUtil.formatElapsedTime(40271)  shouldBe "11:11:11"
    TimeUtil.formatElapsedTime(126671) shouldBe "35:11:11"
    TimeUtil.formatElapsedTime(990671) shouldBe "275:11:11"
  }
}
