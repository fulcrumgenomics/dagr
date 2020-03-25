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
    * number of cores. Will greedily assign the highest number of cores possible.  If the maximum cores is a whole
    * number, then a whole number will be returned, unless the minimum cores (which can be a fractional number of cores)
    * is the only valid value.  If the maximum cores is not a whole number, then the maximum fractional amount will be
    * returned.
    *
    * Example 1: minCores=1, maxCores=5, this.cores=4.5, then 4 cores will be returned.
    * Example 2: minCores=1, maxCores=5.1, this.cores=4.5, then 4.5 cores will be returned.
    * Example 3: minCores=1, maxCores=5, this.cores=1.5, then 1 core will be returned.
    * Example 4: minCores=1.5, maxCores=5, this.cores=1.5, then 1.5 cores will be returned.
    */
  def subset(minCores: Cores, maxCores: Cores, memory: Memory) : Option[ResourceSet] = {
    if (!subsettable(ResourceSet(minCores, memory))) None else {
      val coresValue = {
        // Try to return a whole number value if maxCores is a whole number.  If no whole number exists that is greater
        // than or equal to minCores, then just use minCores (which could be fractional).  If maxCores is fractional,
        // then return a fractional value.
        if (maxCores.value.isValidInt) {
          // Get the number of cores, but rounded down to get a whole number value
          val minValue = Math.floor(Math.min(this.cores.value, maxCores.value))
          // If the number rounded down is smaller than the min-cores, then just return the min-cores
          if (minValue < minCores.value) minCores.value else minValue
        } else { // any fractional number will do
          Math.min(this.cores.value, maxCores.value)
        }
      }
      val resourceSet = ResourceSet(Cores(coresValue), memory)
      require(subsettable(resourceSet))
      Some(resourceSet)
    }
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
