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
package dagr.core.cmdline.parsing

import dagr.core.cmdline.parsing.ParsingUtil._
import dagr.core.cmdline.{CommandLineArgumentEnum, CommandLineException}
import dagr.core.util.StringUtil
import dagr.core.util.StringUtil._

import scala.collection.Map

object ArgumentDefinitionPrinting {

  /** Prints the usage for a given argument definition */
  private[parsing] def printArgumentDefinitionUsage(stringBuilder: StringBuilder,
                                                    argumentDefinition: ArgumentDefinition,
                                                    argumentLookup: ArgumentLookup) {
    printArgumentUsage(stringBuilder,
      argumentDefinition.longName,
      argumentDefinition.shortName,
      argumentDefinition.getTypeDescription,
      makeArgumentDescription(argumentDefinition, argumentLookup))
  }

  /** Gets a string for the given argument definition. */
  private def makeArgumentDescription(argumentDefinition: ArgumentDefinition,
                                      argumentLookup: ArgumentLookup): String = {
    // a secondary map where the keys are the field names
    val sb: StringBuilder = new StringBuilder
    if (argumentDefinition.doc.nonEmpty) sb.append(s"${argumentDefinition.doc}  ")
    if (argumentDefinition.optional) sb.append(makeDefaultValueString(argumentDefinition.defaultValue))
    sb.append(getOptions(argumentDefinition.unitType))
    if (argumentDefinition.mutuallyExclusive.nonEmpty) {
      sb.append(" Cannot be used in conjunction with argument(s): ")
      sb.append(argumentDefinition.mutuallyExclusive.map { targetFieldName =>
        val mutextArgumentDefinition: Option[ArgumentDefinition] = argumentLookup.forField(targetFieldName)
        if (mutextArgumentDefinition.isEmpty) throw new CommandLineException(s"Invalid argument definition in source code (see mutex).  $targetFieldName doesn't match any known argument.")
        mutextArgumentDefinition.get.name + (if (mutextArgumentDefinition.get.shortName.nonEmpty) s" (${mutextArgumentDefinition.get.shortName})" else "")
      }.mkString(", "))
    }
    sb.toString
  }

  /**
    * Intelligently decides whether or not to print a default value. Values are not printed if
    *  a) There is no default
    *  b) There is a default, but it is 'None'
    *  c) There is a default, but it's an empty list
    *  d) There is a default, but it's an empty set
    */
  private def makeDefaultValueString(value : Option[_]) : String = {
    val v = value match {
      case None          => ""
      case Some(None)    => ""
      case Some(Nil)     => ""
      case Some(s) if Set.empty == s => ""
      case Some(c) if c.isInstanceOf[java.util.Collection[_]] && c.asInstanceOf[java.util.Collection[_]].isEmpty => ""
      case Some(Some(x)) => x.toString
      case Some(x)       => x.toString
    }
    if (v.isEmpty) "" else s"[Default: ${v}]. "
  }

  // For formatting argument section of usage message.
  private val ARGUMENT_COLUMN_WIDTH: Int = 30
  private val DESCRIPTION_COLUMN_WIDTH: Int = 90

  /** Prints the usage for a given argument given its various elements */
  private def printArgumentUsage(stringBuilder: StringBuilder, name: String, shortName: String, theType: String, argumentDescription: String) {
    // Desired output: "-f Foo, --foo=Foo" and for Booleans, "-f [true|false] --foo=[true|false]"
    val (shortType, longType) = if (theType == "Boolean") ("[true|false]","[=true|false]") else (theType, "=" + theType)
    var label = ""
    if (shortName.nonEmpty) label += "-" + shortName + " " + shortType + ", "
    label += "--" + name + longType
    stringBuilder.append(wrapString(KGRN, label, KNRM))

    // If the label is short enough, just pad out the column, otherwise wrap to the next line for the description
    var numSpaces: Int = ARGUMENT_COLUMN_WIDTH - label.length
    if (label.length > ARGUMENT_COLUMN_WIDTH) {
      stringBuilder.append("\n")
      numSpaces = ARGUMENT_COLUMN_WIDTH
    }
    stringBuilder.append(" " * numSpaces)

    stringBuilder.append(KCYN)
    val wrappedDescription: String = StringUtil.wordWrap(argumentDescription, DESCRIPTION_COLUMN_WIDTH)
    wrappedDescription.split("\n").zipWithIndex.foreach { case (descriptionLine: String, i: Int) =>
        if (0 < i) stringBuilder.append(" " * ARGUMENT_COLUMN_WIDTH)
      stringBuilder.append(s"$descriptionLine\n")
    }
    stringBuilder.append(s"$KNRM")
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
  private def getOptions(clazz: Class[_]): String = {
    if (clazz.isEnum) {
      val enumClass: Class[_ <: Enum[_ <: Enum[_]]] = clazz.asInstanceOf[Class[_ <: Enum[_ <: Enum[_]]]]
      getEnumOptions(enumClass)
    }
    else ""
  }

  // Also used for Boolean Options
  private val ENUM_OPTION_DOC_PREFIX: String = "Options: "
  private val ENUM_OPTION_DOC_SUFFIX: String = "."

  /** Gets all the options for an enumeration.  If the enumeration extends [[dagr.core.cmdline.CommandLineArgumentEnum]],
    * it will also print the associated help documentation.
    */
  def getEnumOptions(clazz: Class[_ <: Enum[_ <: Enum[_]]]): String = {
    // We assume that clazz is guaranteed to be a Class<? extends Enum>, thus
    // getEnumConstants() won't ever return a null.
    val enumConstants: Array[Enum[_ <: Enum[_]]] = clazz.getEnumConstants.asInstanceOf[Array[Enum[_ <: Enum[_]]]]
    if (enumConstants.length == 0) {
      throw new CommandLineException(s"Bad argument enum type '${clazz.getName}' with no options")
    }

    if (classOf[CommandLineArgumentEnum].isAssignableFrom(clazz)) {
      @SuppressWarnings(Array("unchecked")) val clpEnumCastedConstants: Array[CommandLineArgumentEnum] = enumConstants.asInstanceOf[Array[CommandLineArgumentEnum]]
      (enumConstants zip clpEnumCastedConstants).toList.map { case (c: Enum[_], cCast: CommandLineArgumentEnum) =>
        s"${c.name} (${cCast.getHelpDoc})"
      }.mkString(ENUM_OPTION_DOC_PREFIX, ", ", ENUM_OPTION_DOC_SUFFIX)
    }
    else {
      enumConstants.map(_.name).mkString(ENUM_OPTION_DOC_PREFIX, ", ", ENUM_OPTION_DOC_SUFFIX)
    }
  }
}
