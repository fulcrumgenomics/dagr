/*
 * The MIT License
 *
 * Copyright (c) 2015-2017 Fulcrum Genomics LLC
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


import dagr.api.models.util.{Cores, Memory, ResourceParsing, ResourceSet}
import oshi.SystemInfo
import oshi.hardware.platform.mac.MacHardwareAbstractionLayer

/** The resources needed for an execution system */
object SystemResources extends ResourceParsing {
  private val hal = new SystemInfo().getHardware

  /** Total number of cores in the system */
  val systemCores : Cores =
    if (hal.isInstanceOf[MacHardwareAbstractionLayer])
      Cores(this.hal.getProcessor.getPhysicalProcessorCount)
    else
      Cores(this.hal.getProcessor.getLogicalProcessorCount)

  /** The total amount of memory in the system. */
  val systemMemory: Memory = new Memory(this.hal.getMemory.getTotal)

  /** The heap size of the JVM. */
  val heapSize: Memory = new Memory(Runtime.getRuntime.maxMemory)

  /** Creates a new SystemResources with the specified values. */
  def apply(cores: Double, systemMemory: Long, jvmMemory: Long): SystemResources = {
    new SystemResources(cores = Cores(cores), systemMemory = Memory(systemMemory), jvmMemory = Memory(jvmMemory))
  }

  /** Creates a new SystemResources with the cores provided and partitions the memory between system and JVM. */
  def apply(cores: Option[Cores] = None, totalMemory: Option[Memory] = None) : SystemResources = {
    val heapSize = this.heapSize

    val (system, jvm) = totalMemory match {
      case Some(memory) => (memory, heapSize)
      case None         => (this.systemMemory - heapSize, heapSize)
    }

    require(system.bytes > 0, "System memory cannot be <= 0 bytes.")

    new SystemResources(cores.getOrElse(this.systemCores), system, jvm)
  }

  val infinite: SystemResources = SystemResources(Double.MaxValue, Long.MaxValue, Long.MaxValue)
}

case class SystemResources(cores: Cores, systemMemory: Memory, jvmMemory: Memory) {
  def systemResources: ResourceSet = ResourceSet(cores, systemMemory)
}
