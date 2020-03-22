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
 */

package dagr.core.execsystem

import dagr.core.UnitSpec
import org.scalatest.OptionValues

class ResourceSetTest extends UnitSpec with OptionValues {
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

  it should "subset resources" in {
    // doc examples
    ResourceSet(4.5, 10).subset(Cores(1), Cores(5), Memory(10)).value shouldBe ResourceSet(4, 10)
    ResourceSet(4.5, 10).subset(Cores(1), Cores(5.1), Memory(10)).value shouldBe ResourceSet(4.5, 10)
    ResourceSet(1.5, 10).subset(Cores(1), Cores(5), Memory(10)).value shouldBe ResourceSet(1, 10)
    ResourceSet(1.5, 10).subset(Cores(1.5), Cores(5), Memory(10)).value shouldBe ResourceSet(1.5, 10)

    val resources = ResourceSet(10, 10)
    resources.subset(ResourceSet(10, 10)).value shouldBe ResourceSet(10, 10)
    resources.subset(ResourceSet(10.5, 10)).isDefined shouldBe false
    resources.subset(ResourceSet(9.5, 10)).value shouldBe ResourceSet(9.5, 10)
    resources.subset(ResourceSet(5, 5)).value shouldBe ResourceSet(5, 5)
    resources.subset(ResourceSet(4.5, 5)).value shouldBe ResourceSet(4.5, 5)

    val halfACore = ResourceSet(0.5, 10)
    halfACore.subset(ResourceSet(0.5, 10)).value shouldBe ResourceSet(0.5, 10)
    halfACore.subset(Cores(0.25), Cores(0.5), Memory(10)).value shouldBe ResourceSet(0.5, 10)
    halfACore.subset(Cores(0.5), Cores(1), Memory(10)).value shouldBe ResourceSet(0.5, 10)
    halfACore.subset(Cores(0.1), Cores(1), Memory(10)).value shouldBe ResourceSet(0.1, 10)
    halfACore.subset(Cores(0.51), Cores(1), Memory(10)).isDefined shouldBe false
    halfACore.subset(Cores(0.25), Cores(0.3), Memory(10)).value shouldBe ResourceSet(0.3, 10)

    val fiveAndAHalfCores = ResourceSet(5.5, 10)
    fiveAndAHalfCores.subset(Cores(1.2), Cores(5.8), Memory(10)).value shouldBe ResourceSet(5.5, 10)
  }
}
