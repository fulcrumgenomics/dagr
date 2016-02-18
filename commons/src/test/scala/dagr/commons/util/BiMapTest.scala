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

class BiMapTest extends UnitSpec {
  "BiMap.add" should "remove remove any key or value that already exists" in {
    val map = new BiMap[String, String]()
    map.add("key1", "val1")
    map.containsKey("key1") shouldBe true
    map.containsValue("val1") shouldBe true
    map should have size 1

    map.add("key2", "val1")
    map.containsKey("key1") shouldBe false
    map.containsKey("key2") shouldBe true
    map.containsValue("val1") shouldBe true
    map should have size 1

    map.add("key2", "val2")
    map.containsKey("key1") shouldBe false
    map.containsKey("key2") shouldBe true
    map.containsValue("val1") shouldBe false
    map.containsValue("val2") shouldBe true
    map should have size 1
  }

  "BiMap.removeKey" should "remove the key and associated value from the map, and return true only if it exists" in {
    val map = new BiMap[Int, String]()
    map.add(1, "1")
    map.add(2, "2")
    map.size shouldBe 2
    map.removeKey(0) shouldBe false
    map.size shouldBe 2
    map.removeKey(1) shouldBe true
    map.size shouldBe 1
    map.removeKey(2) shouldBe true
    map.isEmpty shouldBe true
  }

  "BiMap.removeValue" should "remove the value and associated key from the map, and return true only if it exists" in {
    val map = new BiMap[Int, String]()
    map.add(1, "1")
    map.add(2, "2")
    map.size shouldBe 2
    map.removeValue("0") shouldBe false
    map.size shouldBe 2
    map.removeValue("1") shouldBe true
    map.size shouldBe 1
    map.removeValue("2") shouldBe true
    map.isEmpty shouldBe true
  }

  "BiMap.containsKey" should "return true if it contains the key, false otherwise" in {
    val map = new BiMap[Int, String]()
    map.add(1, "1")
    map.add(2, "2")
    map.containsKey(1) shouldBe true
    map.containsKey(2) shouldBe true
    map.containsKey(3) shouldBe false
  }

  "BiMap.keys" should "return the keys" in {
    val map = new BiMap[Int, String]()
    map.add(1, "1")
    map.add(2, "2")
    map.keys shouldBe Set(1, 2).toIterable
  }

  "BiMap.values" should "return the values" in {
    val map = new BiMap[Int, String]()
    map.add(1, "1")
    map.add(2, "2")
    map.values shouldBe Set("1", "2").toIterable
  }
}
