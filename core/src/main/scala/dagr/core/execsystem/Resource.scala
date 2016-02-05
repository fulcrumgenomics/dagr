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

import java.util.regex.{Matcher, Pattern}
import oshi.SystemInfo
import oshi.hardware.platform.mac.MacHardwareAbstractionLayer

/** Manipulates system resources */
object Resource {
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

  /** Get the number of bytes.
    *
    * @param value the number of bytes as a string.
    * @return the number of bytes.
    */
  def parseSizeToBytes(value: String): BigInt = {
    var size: Option[BigInt] = None
    // is it just a number
    try {
      size = Some(BigInt.apply(value))
    } catch {
      case e: Exception =>
    }
    if (size.isDefined) return size.get
    // not a number, so try pattern matching
    val pattern: Pattern = Pattern.compile("([\\d.]+)([PTGMK]B?)", Pattern.CASE_INSENSITIVE)
    val matcher: Matcher = pattern.matcher(value.toLowerCase) // ignore case
    if (!matcher.find()) return -1 // no match found
    val number: String = matcher.group(1)
    var power: String = matcher.group(2)
    if (!power.endsWith("b")) power = power + "b" // for matching below
    val pow: Int = power match {
        case "kb" => 1
        case "mb" => 2
        case "gb" => 3
        case "tb" => 4
        case "pb" => 5
        case _ => 0
      }
    BigInt.apply(number) * BigInt.apply(1024).pow(pow)
  }

  /** Get the number of bytes as a string.
    *
    * @param value        the number of bytes.
    * @param formatSize   the size to normalize the number of bytes (ex. megabytes shouldbe 1024*1024).
    * @param formatSuffix the suffix related to the format size (ex. "k", "m", "g", "t").
    * @return the number of bytes as a string.
    */
  def parseBytesToSize(value: BigInt, formatSize: BigInt = 1024 * 1024, formatSuffix: String = "m"): String = {
    var pow = 0
    var size = value
    while (formatSize * 1024 <= size) {
      pow += 1
      val remainder = 0 //1024 - (size % 1024)
      size = (size + remainder) / 1024
    }
    val remainder = 0 //formatSize - (size % formatSize)
    size = (size + remainder) / formatSize
    size = size * BigInt.apply(1024).pow(pow)
    size.toString + formatSuffix
  }
}

/**
  * Sealed base class that Resources must extend. Requires that resources have a single numeric
  * value, and then provides useful arithmetic and relational operators on that value.
  * @param value the actual value of the resource (e.g. bytes of memory)
  * @param numeric an implicit parameter constraining T to be a numeric type
  * @tparam T the type of number the value is expressed in, e.g. Long or Double
  * @tparam R self-referential type required to make all the operators work nicely
  */
sealed abstract class Resource[T, R <: Resource[T,R]](val value: T)(implicit numeric :Numeric[T]) {
  if (numeric.toDouble(value) <  0) {
    throw new IllegalArgumentException(s"Cannot have negative resource. ${getClass.getSimpleName}=" + value)
  }

  /** Override equals so that we get proper == and != support. */
  override def equals(other: scala.Any): Boolean = {
    null != other && getClass == other.getClass && value == other.asInstanceOf[this.type].value
  }

  /** Subtracts one resource from another. */
  def -(that: R): R = build(numeric.minus(this.value, that.value))
  /** Adds the value of another resource to this resource and returns the new value. */
  def +(that: R): R = build(numeric.plus(this.value, that.value))
  /** Implementation of less than. */
  def <(that: R): Boolean = numeric.lt(this.value, that.value)
  /** Implementation of less than or equal to. */
  def <=(that: R): Boolean = numeric.lteq(this.value, that.value)
  /** Implementation of greater than. */
  def >(that: R): Boolean = numeric.gt(this.value, that.value)
  /** Implementation of greater than or equal to. */
  def >=(that: R): Boolean = numeric.gteq(this.value, that.value)

  /** Must be implemented by subclasses to return a new instance with the specified value. */
  protected def build(value: T) : R
}


/** A resource representing the memory. */
case class Memory(override val value: Long) extends Resource[Long,Memory](value=value) {
  /** Return the number of bytes, as a Long, represented by this object. */
  def bytes: Long = value
  def kb : String = Resource.parseBytesToSize(value, 1024,           "k")
  def mb : String = Resource.parseBytesToSize(value, 1024*1024,      "m")
  def gb : String = Resource.parseBytesToSize(value, 1024*1024*1024, "g")
  def prettyString: String = {
    if (value > 1024*1024*1024) gb
    else if (value > 1024*1024) mb
    else kb
  }

  override protected def build(value: Long): Memory = new Memory(value)
}

/** Companion object for Memory adding some addition apply() methods and defining some useful constant memory values. */
object Memory {
  def apply(stringValue: String): Memory = new Memory(Resource.parseSizeToBytes(stringValue).toLong)
  val none = Memory(0)
  val infinite = Memory(Long.MaxValue) // Not really infinite, but (for 2016) 8192 petabytes of memory seems infinite
}

/** A resource representing a number of cores (including partial cores). */
case class Cores(override val value: Double) extends Resource[Double, Cores](value=value) {
  if (value < 0) throw new IllegalArgumentException("Cannot have negative cores. Cores=" + value)

  /** Round the number of cores to the nearest integer value. */
  def toInt: Int = Math.round(value).toInt

  override protected def build(value: Double): Cores = new Cores(value)
}
