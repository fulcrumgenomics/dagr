/*
 * The MIT License
 *
 * Copyright (c) 2017 Fulcrum Genomics LLC
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

package dagr.core.tasksystem

import dagr.core.UnitSpec
import dagr.api.models.{Cores, Memory, ResourceSet}
import org.scalatest.OptionValues

class SchedulableTest extends UnitSpec with OptionValues {

  private val fixedTask = new FixedResources {
    override def applyResources(resources: ResourceSet) = Unit
  }

  // A variable task that takes twice as much memory in GB as available cores.
  private val doubleMemoryTask = new VariableResources {
    override def pickResources(availableResources: ResourceSet) = {
      val cores = availableResources.cores
      availableResources.subset(cores, Memory(s"${2*cores.value.toLong}g"))
    }
  }

  // A variable task that takes four times as much memory in GB as available cores.
  private val quadMemoryTask = new VariableResources {
    override def pickResources(availableResources: ResourceSet) = {
      val cores = availableResources.cores
      availableResources.subset(cores, Memory(s"${4*cores.value.toLong}g"))
    }
  }

  // A variable task that always takes the available cores and 2g of memory.
  private val twoGbTask = new VariableResources {
    override def pickResources(availableResources: ResourceSet) = availableResources.subset(availableResources.cores, Memory("2g"))
  }

  "Schedulable.minCores" should "work with fixed resources" in {
    fixedTask.requires(1, "2g").minCores().value.value shouldBe 1
    fixedTask.requires(2, "2g").minCores().value.value shouldBe 2
    fixedTask.requires(2, "2g").minCores(end=Cores(1)) shouldBe 'empty
    fixedTask.requires(2, "2g").minCores(start=Cores(1), end=Cores(2), step=Cores(2)) shouldBe 'empty
  }

  it should "work with variable resources" in {
    doubleMemoryTask.minCores(end=Cores(1)).value.value shouldBe 1
    doubleMemoryTask.minCores(start=Cores(2), end=Cores(2), step=Cores(1)).value.value shouldBe 2
    doubleMemoryTask.minCores(start=Cores(3), end=Cores(2), step=Cores(1)) shouldBe 'empty
  }

  "Schedulable.minMemory" should "work with fixed resources" in {
    fixedTask.requires(1, "2g").minMemory().value shouldBe Memory("2g")
    fixedTask.requires(2, "2g").minMemory().value shouldBe Memory("2g")
    fixedTask.requires(2, "2g").minMemory(end=Memory("1g")) shouldBe 'empty
    fixedTask.requires(2, "2g").minMemory(start=Memory("1g"), end=Memory("2g"), step=Memory("2g")) shouldBe 'empty
  }

  it should "work with variable resources" in {
    twoGbTask.minMemory().value.value shouldBe Memory("2g").value
    twoGbTask.minMemory(end=Memory("1g"), step=Memory("256m")) shouldBe 'empty
    twoGbTask.minMemory(end=Memory("2g"), step=Memory("256m")).value.value shouldBe Memory("2g").value
    twoGbTask.minMemory(start=Memory("1g"), end=Memory("2g"), step=Memory("1g")).value.value shouldBe Memory("2g").value
    twoGbTask.minMemory(start=Memory("3g"), end=Memory("2g"), step=Memory("1g")) shouldBe 'empty
  }

  "Schedulable.minCoresAndMemory" should "work with fixed resources" in {
    fixedTask.requires(2, "2g").minCoresAndMemory(memoryPerCore=Memory("1g")).value shouldBe ResourceSet(Cores(2), Memory("2g"))
    fixedTask.requires(2, "1g").minCoresAndMemory(memoryPerCore=Memory("512m")).value shouldBe ResourceSet(Cores(2), Memory("1g"))
    fixedTask.requires(2, "1g").minCoresAndMemory(memoryPerCore=Memory(Memory("1m").value / 2)) shouldBe 'empty
  }

  it should "work with variable resources" in {
    doubleMemoryTask.minCoresAndMemory(memoryPerCore=Memory("4g")).value shouldBe ResourceSet(Cores(1), Memory("2g"))
    doubleMemoryTask.minCoresAndMemory(memoryPerCore=Memory("2g")).value shouldBe ResourceSet(Cores(1), Memory("2g"))
    doubleMemoryTask.minCoresAndMemory(memoryPerCore=Memory("1g")) shouldBe 'empty

    quadMemoryTask.minCoresAndMemory(memoryPerCore=Memory("4g")).value shouldBe ResourceSet(Cores(1), Memory("4g"))
    quadMemoryTask.minCoresAndMemory(memoryPerCore=Memory("2g")) shouldBe 'empty
    quadMemoryTask.minCoresAndMemory(memoryPerCore=Memory("1g")) shouldBe 'empty
  }

  "Schedulable.minResources" should "work with fixed resources" in {
    fixedTask.requires(1, "2g").minResources().value shouldBe ResourceSet(Cores(1), Memory("2g"))
    fixedTask.requires(2, "24g").minResources().value shouldBe ResourceSet(Cores(2), Memory("24g"))
    fixedTask.requires(2, "24g").minResources(maximumResources=ResourceSet(Cores(1), Memory("24g"))) shouldBe 'empty
  }

  it should "work with variable resources" in {
    twoGbTask.minResources().value shouldBe ResourceSet(Cores(1), Memory("2g"))
    twoGbTask.minResources(ResourceSet(Cores(2), Memory("2g"))).value shouldBe ResourceSet(Cores(1), Memory("2g"))
    twoGbTask.minResources(ResourceSet(Cores(2), Memory("1g"))).value shouldBe ResourceSet(Cores(2), Memory("2g"))
    twoGbTask.minResources(ResourceSet(Cores(2), Memory("1023m"))) shouldBe 'empty

    doubleMemoryTask.minResources().value shouldBe ResourceSet(Cores(1), Memory("2g"))
    doubleMemoryTask.minResources(ResourceSet(Cores(2), Memory("1g"))) shouldBe 'empty

    quadMemoryTask.minResources().value shouldBe ResourceSet(Cores(1), Memory("4g"))
    quadMemoryTask.minResources(ResourceSet(Cores(1), Memory("4g"))).value shouldBe ResourceSet(Cores(1), Memory("4g"))
    quadMemoryTask.minResources(ResourceSet(Cores(1), Memory("3g"))) shouldBe 'empty
  }
}
