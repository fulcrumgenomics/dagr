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

package dagr.sopt.util

import java.lang.reflect.Modifier

import dagr.commons.reflect.ReflectionUtil
import dagr.commons.util.{StringUtil, ClassFinder}
import dagr.sopt.cmdline._
import dagr.sopt.clp

import scala.collection.JavaConversions._
import scala.collection.immutable.Map
import scala.collection.mutable
import scala.reflect._
import scala.reflect.runtime.universe._

/** Variables and Methods to support command line parsing */
object ParsingUtil {
  /** Gets the [[clp]] annotation on this class */
  def findClpAnnotation(clazz: Class[_]): Option[clp] = {
    ReflectionUtil.findJavaAnnotation(clazz, classOf[clp])
  }

  /** Gets a mapping between sub-classes of [[T]] and their [[clp]] annotation.
    *
    * @param srcClasses the source classes from which to build the map.
    * @param omitSubClassesOf a list of omit subclasses to omit from the returned map.
    * @param includeHidden include classes whose [[clp]] annotation has `hidden` set to true.
    * @tparam T the super class.
    * @return a mapping between sub-classes of [[T]] and their [[clp]] annotation.
    */
  private def classToAnnotationMapFromSourceClasses[T](srcClasses: Traversable[Class[_]],
                                                     omitSubClassesOf: Iterable[Class[_]] = Nil,
                                                     includeHidden: Boolean = false)
  : Map[Class[_ <: T], clp] = {

    // Filter out interfaces, synthetic, primitive, local, or abstract classes.
    def keepClass(clazz: Class[_]): Boolean = {
      !clazz.isInterface && !clazz.isSynthetic && !clazz.isPrimitive && !clazz.isLocalClass && !Modifier.isAbstract(clazz.getModifiers)
    }

    // Find all classes with the annotation
    val classes: Traversable[Class[T]] = srcClasses
      .filter { keepClass }
      .filterNot { clazz => omitSubClassesOf.exists { _.isAssignableFrom(clazz) } }
      .filter {
        findClpAnnotation(_) match {
          case None      => false
          case Some(clp) => includeHidden || !clp.hidden
        }
      }.map(_.asInstanceOf[Class[T]])

    // Get all the name collisions
    val nameCollisions = classes
      .groupBy(_.getSimpleName)
      .filter { case (name, cs) => cs.size > 1 }
      .map    { case (name, cs) => cs.mkString(", ") }

    // SimpleName should be unique
    if (nameCollisions.nonEmpty) {
      throw new CommandLineException(s"Simple class name collision: ${nameCollisions.mkString(", ")}")
    }

    // Finally, make the map
    classes.map(clazz => Tuple2(clazz, ReflectionUtil.findJavaAnnotation(clazz, classOf[clp]).get)).toMap
  }


  /** Finds all class that extends [[T]] and produces a map from a sub-class of [[T]] to its [[clp]] annotation.
  *
  * Throws a [[IllegalStateException]] if a sub-class of [[T]] is missing the [[clp]] annotation.
  *
  * @param packageList the namespace(s) to search.
  * @param omitSubClassesOf a list of omit subclasses to omit from the returned map.
  * @param includeHidden include classes whose [[clp]] annotation has `hidden` set to true.
  * @return map from clazz to property.
  */
  def findClpClasses[T: ClassTag: TypeTag](packageList: List[String],
                                  omitSubClassesOf: Iterable[Class[_]] = Nil,
                                  includeHidden: Boolean = false)
  : Map[Class[_ <: T], clp] = {
    val clazz: Class[T] = ReflectionUtil.typeToClass(typeOf[T]).asInstanceOf[Class[T]]

    // find all classes that extend [[T]]
    val classFinder: ClassFinder = new ClassFinder
    for (pkg <- packageList) {
      classFinder.find(pkg, clazz)
    }

    classToAnnotationMapFromSourceClasses[T](
      srcClasses = classFinder.getClasses,
      omitSubClassesOf = omitSubClassesOf,
      includeHidden = includeHidden)
  }

  /** When a command does not match any known command, searches for similar commands, using the same method as GIT **/
  private def findSimilar(target: String,
                          options: Traversable[String],
                          unknownSimilarityFloor: Int = 7,
                          unknownSubstringLength: Int = 5
                         ): Seq[String] = {
    val distances: mutable.Map[String, Integer] = new mutable.HashMap[String, Integer]
    var bestDistance: Int = Integer.MAX_VALUE
    var bestN: Int = 0
    options.foreach{ name =>
      if (name == target) {
        throw new IllegalStateException(s"BUG: Option name matches when searching for the unknown: $name")
      }
      val distance: Int = if (name.startsWith(target) || (unknownSubstringLength <= target.length && name.contains(target))) {
        0
      }
      else {
        StringUtil.levenshteinDistance(target, name, 0, 2, 1, 4)
      }
      distances.put(name, distance)
      if (distance < bestDistance) {
        bestDistance = distance
        bestN = 1
      }
      else if(distance == bestDistance) {
        bestN += 1
      }
    }
    if (0 == bestDistance && 1 < bestN && bestN == options.size) {
      bestDistance = unknownSimilarityFloor + 1
    }
    if (bestDistance < unknownSimilarityFloor) {
      options.filter(bestDistance == distances.get(_).get).toSeq
    }
    else {
      Seq.empty[String]
    }
  }

  /** Finds all options that are similar to the target and returns a string of suggestions if any were found. */
  private[sopt] def printUnknown(target: String,
                                 options: Traversable[String],
                                 unknownSimilarityFloor: Int = 7,
                                 unknownSubstringLength: Int = 5): String = {
    findSimilar(target=target, options=options, unknownSimilarityFloor=unknownSimilarityFloor, unknownSubstringLength=unknownSubstringLength) match {
      case Nil => ""
      case suggestions =>
        val optionSeparator = "\n        "
        String.format("\nDid you mean %s?%s",
          if (suggestions.length < 2) "this" else "one of these",
          optionSeparator + suggestions.mkString(optionSeparator)
        )
    }
  }
}
