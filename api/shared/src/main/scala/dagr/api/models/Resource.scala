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

package dagr.api.models

/** Manipulates system resources */
trait ResourceParsing {

  /** Get the number of bytes.
    *
    * @param value the number of bytes as a string.
    * @return the number of bytes.
    */
  def parseSizeToBytes(value: String): BigInt = {
    // is it just a number
    val size: Option[BigInt] = try { Some(BigInt.apply(value)) } catch { case e: NumberFormatException => None }
    size getOrElse {
      val regex = """([\d\.]+)([ptgmk])b?""".r
      value.toLowerCase match {
        case regex(number, power) =>
          val pow: Int = power match {
            case "k" => 1
            case "m" => 2
            case "g" => 3
            case "t" => 4
            case "p" => 5
            case _ => 0
          }
          BigInt(number) * BigInt(1024).pow(pow)
        case _ => -1
      }
    }
  }

  /** Get the number of bytes as a string.
    *
    * @param value        the number of bytes.
    * @param formatSize   the size to normalize the number of bytes (ex. megabytes should be 1024*1024).
    * @param formatSuffix the suffix related to the format size (ex. "k", "m", "g", "t").
    * @return the number of bytes as a string.
    */
  def parseBytesToSize(value: BigInt, formatSize: BigInt = 1024 * 1024, formatSuffix: String = "m"): String = {
    if (value > formatSize) {
      (value / formatSize).toString + formatSuffix
    }
    else {
      f"${BigDecimal(value) / BigDecimal(formatSize)}%.2f".reverse.dropWhile(_ == '0').dropWhile(_ == '.').reverse + formatSuffix
    }
  }
}

object Resource extends ResourceParsing

/**
  * Sealed base class that Resources must extend. Requires that resources have a single numeric
  * value, and then provides useful arithmetic and relational operators on that value.
  * @param value the actual value of the resource (e.g. bytes of memory)
  * @param numeric an implicit parameter constraining T to be a numeric type
  * @tparam T the type of number the value is expressed in, e.g. Long or Double
  * @tparam R self-referential type required to make all the operators work nicely
  */
sealed abstract class Resource[T, R <: Resource[T,R]](val value: T)(implicit numeric :Numeric[T]) {
  if (numeric.toDouble(value) < 0) {
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

  /** Make the toString always return the value as a string. */
  final override def toString: String = String.valueOf(value)

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

/** Companion object for Memory adding some additional apply() methods and defining some useful constant memory values. */
object Memory {
  def apply(stringValue: String): Memory = new Memory(Resource.parseSizeToBytes(stringValue).toLong)
  val none = Memory(0)
  val infinite = Memory(Long.MaxValue) // Not really infinite, but (for 2016) 8192 petabytes of memory seems infinite
}

/** A resource representing a number of cores (including partial cores). */
case class Cores(override val value: Double) extends Resource[Double, Cores](value=value) {

  /** Round the number of cores to the nearest integer value. */
  def toInt: Int = Math.round(value).toInt

  override protected def build(value: Double): Cores = new Cores(value)
}

/** Companion object for Core adding an additional apply() method. */
object Cores {
  def apply(cores: Cores): Cores = new Cores(cores.value)
  val none = Cores(0.0)
  val infinite = Cores(Double.MaxValue) // Not really infinite, but close to it.
}