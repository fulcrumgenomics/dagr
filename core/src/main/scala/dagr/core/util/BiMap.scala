/*
 * The MIT License
 *
 * Copyright (c) 2015 Fulcrum Genomics LLC
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
package dagr.core.util

import scala.collection.mutable

/** An iterable bi-Directional map.
 *
 * @tparam K the key type.
 * @tparam V the value type.
 */
class BiMap[K, V]() extends Iterable[(K, V)] {
  private val forward = new mutable.HashMap[K, V]()
  private val reverse = new mutable.HashMap[V, K]()

  /** Adds a tuple to the map.
   *
   * @param key the key.
   * @param value the value.
   */
  def add(key: K, value: V): Unit = {
    forward.put(key, value)
    reverse.put(value, key)
  }

  /** Get a key associated with the given value.
   *
   * @param value the value to lookup.
   * @return the key associated with the value, None if not found.
   */
  def keyFor(value: V): Option[K] = reverse.get(value)

  /** Get a value associated with a given key.
   *
   * @param key the key to lookup.
   * @return the value associated with the key, None if not found.
   */
  def valueFor(key: K): Option[V] = forward.get(key)

  /** Checks if the map contains the given key.
   *
   * @param key the key to check.
   * @return true if the key is in the map, false otherwise.
   */
  def containsKey(key: K): Boolean = forward.contains(key)

  /** Checks if the map contains the given value.s
   *
   * @param value the value to check.
   * @return true if the value is in the map, false otherwise.
   */
  def containsValue(value: V): Boolean = reverse.contains(value)

  /** Remove a key and associated value from the map.
   *
   * @param key the key to remove.
   * @return true if the key and associated value were removed successfully, false otherwise.
   */
  def removeKey(key: K): Boolean = {
    val value = valueFor(key)
    if (value.isEmpty) {
      false
    }
    else {
      forward.remove(key)
      reverse.remove(value.get)
      true
    }
  }

  /** Remove a value and associated key from the map.
    *
    * @param value the value to remove.
    * @return true if the value and associated key were removed successfully, false otherwise.
    */
  def removeValue(value: V): Boolean = {
    val key = keyFor(value)
    if (key.isEmpty) {
      false
    }
    else {
      forward.remove(key.get)
      reverse.remove(value)
      true
    }
  }

  /**
   *
   * @return the keys in this map.
   */
  def keys: Iterable[K] = forward.keys

  /**
   *
   * @return the values in this map.
   */
  def values: Iterable[V] = reverse.keys

  /**
   *
   * @return the number of tuples in this map.
   */
  override def size: Int = forward.size

  /**
   *
   * @return an iterator over tuples in this map.
   */
  def iterator: Iterator[(K, V)] = {
    forward.iterator
  }
}
