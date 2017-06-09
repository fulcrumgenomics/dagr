/*
 * The MIT License
 *
 * Copyright (c) $year Fulcrum Genomics
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

class ResourceSetTest extends UnitSpec {
  "ResourceSet.isEmpty" should "return true for the empty resource set" in {
    ResourceSet.empty.isEmpty shouldBe true
  }

  "ResourceSet" should "add and subtract resources" in {
    val original = ResourceSet(10, 10)
    val middle = ResourceSet(original)
    var running = original + middle
    running.cores.value shouldBe 20
    running.memory.value shouldBe 20
    running = running - middle
    running.cores.value shouldBe 10
    running.memory.value shouldBe 10
    running = running - Cores(10)
    running.cores.value shouldBe 0
  }
}
