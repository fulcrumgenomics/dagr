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

package dagr.sopt.cmdline

import java.util

import dagr.commons.reflect.ReflectionUtil
import dagr.sopt.util._
import dagr.commons.util.StringUtil

import scala.util.{Failure, Success}

object ClpArgumentDefinitionPrinting {

  /** Strings for printing enum options */
  private[cmdline] val EnumOptionDocPrefix: String = "Options: "
  private[cmdline] val EnumOptionDocSuffix: String = "."

  /** Prints the usage for a given argument definition */
  private[cmdline] def printArgumentDefinitionUsage(stringBuilder: StringBuilder,
                                                    argumentDefinition: ClpArgument,
                                                    argumentLookup: ClpArgumentLookup): Unit = {
    printArgumentUsage(stringBuilder,
      argumentDefinition.longName,
      argumentDefinition.shortName,
      argumentDefinition.typeDescription,
      makeCollectionArity(argumentDefinition),
      makeArgumentDescription(argumentDefinition, argumentLookup))
  }

  def mutexErrorHeader: String = " Cannot be used in conjunction with argument(s): "

  /** Gets a string for the given argument definition. */
  private def makeArgumentDescription(argumentDefinition: ClpArgument,
                                      argumentLookup: ClpArgumentLookup): String = {
    // a secondary map where the keys are the field names
    val sb: StringBuilder = new StringBuilder
    if (argumentDefinition.doc.nonEmpty) sb.append(s"${argumentDefinition.doc}  ")
    if (argumentDefinition.optional) sb.append(makeDefaultValueString(argumentDefinition.defaultValue))
    sb.append(possibleValues(argumentDefinition.unitType))

    if (argumentDefinition.mutuallyExclusive.nonEmpty) {
      sb.append(mutexErrorHeader)
      sb.append(argumentDefinition.mutuallyExclusive.map { targetFieldName =>
        argumentLookup.forField(targetFieldName) match {
          case None =>
            throw new UserException(s"Invalid argument definition in source code (see mutex). " +
              s"$targetFieldName doesn't match any known argument.")
          case Some(mutex) =>
            mutex.name + (if (mutex.shortName.nonEmpty) s" (${mutex.shortName})" else "")
        }
      }.mkString(", "))
    }
    sb.toString
  }

  private def makeCollectionArity(argumentDefinition: ClpArgument): Option[String] = {
    if (!argumentDefinition.isCollection) return None

    val desciption = (argumentDefinition.minElements, argumentDefinition.maxElements) match {
      case (0, Integer.MAX_VALUE) => "*"
      case (1, Integer.MAX_VALUE) => "+"
      case (m, n)                 => s"{${m}..${n}}"
    }
    Some(desciption)
  }

  /**
    * Intelligently decides whether or not to print a default value. Values are not printed if
    *  a) There is no default
    *  b) There is a default, but it is 'None'
    *  c) There is a default, but it's an empty list
    *  d) There is a default, but it's an empty set
    */
  private[cmdline] def makeDefaultValueString(value : Option[_]) : String = {
    import scala.collection.JavaConversions.iterableAsScalaIterable
    val v = value match {
      case None          => ""
      case Some(None)    => ""
      case Some(Nil)     => ""
      case Some(s) if Set.empty == s => ""
      case Some(c) if c.isInstanceOf[util.Collection[_]]  => c.asInstanceOf[util.Collection[_]].mkString(", ")
      case Some(t) if t.isInstanceOf[Traversable[_]]      => t.asInstanceOf[Traversable[_]].mkString(", ")
      case Some(Some(x)) => x.toString
      case Some(x)       => x.toString
    }
    if (v.isEmpty) "" else s"[Default: $v]. "
  }

  // For formatting argument section of usage message.
  private val ArgumentColumnWidth: Int = 30
  private val DescriptionColumnWidth: Int = 90

  /** Prints the usage for a given argument given its various elements */
  private[cmdline] def printArgumentUsage(stringBuilder: StringBuilder, name: String, shortName: String, theType: String,
                                          collectionArityString: Option[String], argumentDescription: String): Unit = {
    // Desired output: "-f Foo, --foo=Foo" and for Booleans, "-f [true|false] --foo=[true|false]"
    val collectionDesc = collectionArityString.getOrElse("")
    val (shortType, longType) = if (theType == "Boolean") ("[true|false]","[=true|false]") else (theType, "=" + theType)
    val label = new StringBuilder()
    if (shortName.nonEmpty) label.append("-" + shortName + " " + shortType + collectionDesc + ", ")
    label.append("--" + name + longType + collectionDesc)
    stringBuilder.append(KGRN(label.toString()))

    // If the label is short enough, just pad out the column, otherwise wrap to the next line for the description
    val numSpaces: Int =  if (label.length > ArgumentColumnWidth) {
      stringBuilder.append("\n")
      ArgumentColumnWidth
    }
    else {
      ArgumentColumnWidth - label.length
    }
    stringBuilder.append(" " * numSpaces)

    val wrappedDescriptionBuilder = new StringBuilder()
    val wrappedDescription: String = StringUtil.wordWrap(argumentDescription, DescriptionColumnWidth)
    wrappedDescription.split("\n").zipWithIndex.foreach { case (descriptionLine: String, i: Int) =>
        if (0 < i) wrappedDescriptionBuilder.append(" " * ArgumentColumnWidth)
      wrappedDescriptionBuilder.append(s"$descriptionLine\n")
    }
    stringBuilder.append(KCYN(wrappedDescriptionBuilder.toString()))
  }

  /**
    * Returns the help string with details about valid options for the given argument class.
    *
    * <p>
    * Currently this only make sense with [[Boolean]] and [[Enumeration]]. Any other class
    * will result in an empty string.
    * </p>
    *
    * @param clazz the target argument's class.
    * @return never { @code null}.
    */
  private def possibleValues(clazz: Class[_]): String = {
    if (clazz.isEnum) {
      val enumClass: Class[_ <: Enum[_ <: Enum[_]]] = clazz.asInstanceOf[Class[_ <: Enum[_ <: Enum[_]]]]
      val enumConstants = ReflectionUtil.enumOptions(enumClass) match {
        case Success(constants) => constants
        case Failure(thr) => throw thr
      }
      enumConstants.map(_.name).mkString(EnumOptionDocPrefix, ", ", EnumOptionDocSuffix)
    }
    else ""
  }
}
