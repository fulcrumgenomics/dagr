/*
 * The MIT License
 *
 * Copyright (c) 2015-6 Fulcrum Genomics LLC
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

object ResourceSet {
  def apply(that: ResourceSet): ResourceSet = new ResourceSet(cores = Cores(that.cores.value), memory = Memory(that.memory.value))
  def apply(cores: Double, memory: Long): ResourceSet = new ResourceSet(Cores(cores), Memory(memory))

  val empty = ResourceSet(0, 0)
  val infinite = ResourceSet(Double.MaxValue, Long.MaxValue)
}

/** Holds information about a set of resources */
case class ResourceSet(cores: Cores = Cores.none, memory: Memory = Memory.none) {
  def isEmpty: Boolean = cores.value == 0 && memory.value == 0

  override def toString: String = s"memory=$memory, cores=${cores.value}"

  /** Returns true if `subset` is a subset of this resource set and false otherwise. */
  private def subsettable(subset: ResourceSet) = subset.cores <= this.cores && subset.memory <= this.memory

  /**
    * Constructs a resource set with the provided cores and memory, providing that the cores
    * and memory are a subset of those in the current resource set. If the value do not represent
    * a subset, None is returned.
    */
  def subset(that: ResourceSet): Option[ResourceSet] = if (subsettable(that)) Some(that) else None

  /**
    * Constructs a resource set with the provided cores and memory, providing that the cores
    * and memory are a subset of those in the current resource set. If the value do not represent
    * a subset, None is returned.
    */
  def subset(cores: Cores, memory: Memory) : Option[ResourceSet] = subset(ResourceSet(cores, memory))

  /**
    * Constructs a subset of this resource set with a fixed amount of memory and a variable
    * number of cores. Will greedily assign the highest number of cores possible.
    */
  def subset(minCores: Cores, maxCores: Cores, memory: Memory) : Option[ResourceSet] = {
    val min = minCores.value
    val max = maxCores.value
    val cores = max.to(min, -1).find(cores => subset(Cores(cores), memory).isDefined)
    cores.map(c => ResourceSet(Cores(c), memory))
  }

  /**
    * Returns a [[dagr.core.execsystem.ResourceSet]] with the remaining resources after subtracting `other`
    * if doing so would not generate negative resources. Otherwise returns `None`.
    */
  def minusOption(other: ResourceSet) : Option[ResourceSet] = if (subsettable(other)) Some(this - other) else None

  /** Constructs a new resource set by subtracting the provided resource set from this one. */
  def -(that: ResourceSet) : ResourceSet = ResourceSet(this.cores.value - that.cores.value, this.memory.value - that.memory.value)

  /** Constructs a new resource set by subtracting the provided resource set from this one. */
  def -(that: Cores) : ResourceSet = ResourceSet(this.cores.value - that.value, this.memory.value)

  /** Constructs a new resource set by subtracting the provided resource set from this one. */
  def +(that: ResourceSet) : ResourceSet = ResourceSet(this.cores.value + that.cores.value, this.memory.value + that.memory.value)
}
