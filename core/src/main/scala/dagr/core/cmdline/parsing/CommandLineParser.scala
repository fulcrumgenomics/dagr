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

import java.lang.reflect.Field

import dagr.core.cmdline._
import dagr.core.cmdline.parsing.ParseResult.ParseResult
import dagr.core.tasksystem.ValidationException
import dagr.core.util.LazyLogging
import dagr.sopt.{OptionParsingException, IllegalOptionNameException, OptionParser}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.{Failure, Success}

/** Stores strings used in the usage and error messages */
private object CommandLineParserStrings {
  val REQUIRED_ARGUMENTS = "Required Arguments:"
  val OPTIONAL_ARGUMENTS = "Optional Arguments:"
  val USAGE_PREFIX = "USAGE:"
  val COMMENT = "#"

  def getRequiredArgument(fullName: String): String = {
    s"Argument '$fullName' is required"
  }

  def getRequiredArgumentWithMutex(fullName: String, argumentDefinition: ArgumentDefinition): String = {
    s"${getRequiredArgument(fullName)}" + (if (argumentDefinition.mutuallyExclusive.isEmpty) "." else s" unless any of ${argumentDefinition.mutuallyExclusive} are specified.")
  }
}

/** Enumeration that represents the possible outcomes of trying to parse a command line. */
object ParseResult extends Enumeration {
  type ParseResult = Value
  val Success = Value // Successful parse
  val Failure = Value // Something went wrong (unknown option, missing arg, etc.)
  val Help    = Value // A require for command line help was encountered
  val Version = Value // A request to display version was encountered
}

object CommandLineParser {
  private[parsing] def parseArgumentsFile(lines: Traversable[String]): Traversable[String] = {
    val args: ListBuffer[String] = new ListBuffer[String]
    lines.foreach { line =>
      if (!line.startsWith(CommandLineParserStrings.COMMENT) && line.trim.nonEmpty) {
        // split apart by spaces, but all neighbouring strings without a leading dash ('-') should be grouped
        // ex. => "--aab s --b -c -d wer we -c"
        val tokens: List[String] = line.split(" ").filter(_.nonEmpty).filter(0 != _.compareTo(" ")).toList
        // ex. => "--aab" "s" "--b" "-c" "-d" "wer" "we" "-c"
        if (tokens.nonEmpty) {
          var prev: String = tokens.head
          for (i <- 1 until tokens.size) {
            if (!prev.startsWith("-") && !tokens(i).startsWith("-")) {
              prev += s" ${tokens(i)}"
            }
            else {
              args += prev
              prev = tokens(i)
            }
          }
          args += prev
          // ex. => "--aab" "s" "--b" "-c" "-d" "wer we" "-c"
        }
      }
    }
    args.toList
  }


}

/******************************************************************************
  * Class for managing the parsing of command line arguments and matching them
  * to annotated constructor parameters on Tasks.
  *****************************************************************************/
private[cmdline] class CommandLineParser[T](val targetClass: Class[T]) extends LazyLogging {
  import CommandLineParserStrings._
  import ParsingUtil._
  import dagr.core.util.StringUtil._

  private[parsing] val argumentLookup: ArgumentLookup = new ArgumentLookup()
  private val argumentsFilesLoadedAlready: mutable.Set[String] = new mutable.HashSet[String]

  /** The instance of type T that is None initially, and will be populated with a T after a successful parse(). */
  var instance : Option[T] = None
  /** The special args object that is None initially, and will be populated with a value after a successful parse(). */
  var specialArguments : Option[SpecialArgumentsCollection] = None

  // Actual constructor code here!
  createArgumentDefinitions(classOf[SpecialArgumentsCollection], this.argumentLookup)
  createArgumentDefinitions(targetClass, this.argumentLookup)

  protected def targetName: String = targetClass.getSimpleName

  /**
    * Creates argument definitions for all the fields in the provided class, and stores them in the
    * provided ArgumentLookup.
    */
  private[parsing] def createArgumentDefinitions(declaringClass: Class[_], lookup: ArgumentLookup): Unit = {
    new ReflectionHelper(declaringClass).argumentLookup.view.foreach(lookup.add)

    // set mutex arguments
    lookup.ordered.filterNot(_.omitFromCommandLine).foreach { argumentDefinition: ArgumentDefinition =>
      val argumentAnnotation = argumentDefinition.annotation
      val sourceFieldName = argumentDefinition.name
      argumentAnnotation.get.mutex.foreach { targetFieldName =>
        val mutexArgumentDef: Option[ArgumentDefinition] = lookup.forField(targetFieldName)
        if (mutexArgumentDef.isDefined) {
          mutexArgumentDef.get.mutuallyExclusive.add(sourceFieldName)
        }
        else {
          throw new BadAnnotationException(s"Bad Arg annotation in field '$sourceFieldName': mutex field '$targetFieldName' was not found in the list of fields.")
        }
      }
    }
  }

  /**
    * Parses the command line arguments for a given class and attempts to instantiate the class.
    * Appends any errors to the `stringBuilder` if parsing is unsuccessful.
    *
    * @param errorMessageBuilder the string builder in which to store argument error messages
    * @param args the args to parse.
    * @return true if we successfully parsed the args, false otherwise
    */
  def parseAndBuild(errorMessageBuilder: StringBuilder, args: Array[String]): ParseResult = {
    // Try parsing the arguments
    if (!parseArgs(errorMessageBuilder = errorMessageBuilder, args = args)) {
      return ParseResult.Failure
    }

    // Then build the instances
    try {
      val (specials, normals) = this.argumentLookup.ordered.partition(p => p.declaringClass == classOf[SpecialArgumentsCollection])
      val specialHelper = new ReflectionHelper(classOf[SpecialArgumentsCollection])
      this.specialArguments = Some(specialHelper.build(specials.map(d => d.value.get)))
      this.specialArguments.foreach(special => {
        if (special.version) return ParseResult.Version
        if (special.help) return ParseResult.Help
      })

      assertArgumentsAreValid(this.argumentLookup)
      val normalHelper = new ReflectionHelper(targetClass)
      this.instance = Some(normalHelper.build(normals.map(d => d.value.orNull)))
      ParseResult.Success
    }
    catch {
      case ValidationException(xs) => xs.foreach {errorMessageBuilder.append("\n").append(_).append("\n")}; ParseResult.Failure
      case e: Exception => errorMessageBuilder.append(s"\n${e.getClass.getSimpleName}: ${e.getMessage}\n"); ParseResult.Failure
    }
  }

  /**
    * Parses the command line recursively, by parsing the arguments in the args array, loading any arguments files
    * indicated, and then repeating again for each file.
    */
  private def parseArgs(errorMessageBuilder: StringBuilder, args: Array[String]): Boolean = {
    try {
      val parser: OptionParser = new OptionParser

      // Add to the option parsers
      this.argumentLookup.ordered.filterNot(_.omitFromCommandLine).foreach {
        case arg if arg.isFlag =>        parser.acceptFlag(          arg.getNames: _*)
        case arg if !arg.isCollection => parser.acceptSingleValue(   arg.getNames: _*)
        case arg =>                      parser.acceptMultipleValues(arg.getNames: _*)
      }

      // Parse the args
      parser.parse(args: _*) match {
        case Success(_) =>
        case Failure(throwable) => throw throwable // TODO: should we wrap this in CommandLineException?
      }

      // Check for the special arguments file flag
      // If it's seen, read arguments from that file and recursively call parseArgsWithDefinitions()
      /*
      if (parser.hasOptionValues(SpecialArgumentsCollection.ARGUMENTS_FILE_FULLNAME)) {
        val argFiles: List[String] = parser.getOptionValues(SpecialArgumentsCollection.ARGUMENTS_FILE_FULLNAME) match {
          case Success(list) => list
          case Failure(throwable) => throw throwable // TODO: should we wrap this in CommandLineException?
        }
        var newArgs: List[String] = argFiles.distinct.filter(file => !argumentsFilesLoadedAlready.contains(file)).flatMap { file =>
          argumentsFilesLoadedAlready.add(file)
          loadArgumentsFile(file)
        }
        argumentsFilesLoadedAlready.addAll(argFiles)
        if (newArgs.nonEmpty) {
          newArgs ++= args.toList
          return parseArgs(usageBuilder, errorMessageBuilder, newArgs.toArray, withPreamble)
        }
      }
      */

      // set the values
      parser.foreach {
        case (name: String, values: List[String]) =>
          this.argumentLookup.forArg(name).get.setArgument(values)
        case _ =>
          throw new IllegalStateException("Parser returned an unexpected set of values")
      }

      true
    }
    catch {
      case ex: OptionParsingException => errorMessageBuilder.append(s"\n${ex.getMessage}\n"); false
      case ex : CommandLineException => errorMessageBuilder.append(s"\n${ex.toString}\n"); false
      case ex: Exception => errorMessageBuilder.append(s"\n${ex.toString}\n" + ex.getStackTrace.mkString("\t\n")); false
    }
  }

  /**
    * A typical command line program will call this to get the beginning of the usage message,
    * and then append a description of the program, like this:

    */
  protected def getStandardUsagePreamble: String = s"$USAGE_PREFIX " + targetName + " [arguments]\n\n"

  /**
    * Print a usage message for a given command line task.
    *
    * @param printCommon True if common args should be included in the usage message.
    */
  def usage(printCommon: Boolean = true, withPreamble: Boolean = true, withSpecial: Boolean = true): String = {
    val builder = new StringBuilder()

    if (withPreamble) {
      builder.append(wrapString(KRED, s"$getStandardUsagePreamble", KNRM))
      builder.append(wrapString(KRED, s"$version\n", KNRM))
    }

    // filter on common and partition on optional
    val (required, optional) = argumentLookup.view
      .filterNot { _.omitFromCommandLine }
      .filter { printCommon || !_.isCommon }
      .filter { withSpecial || !_.isSpecial }
      .partition { !_.optional }

    if (required.nonEmpty) {
      builder.append(wrapString(KRED, s"\n$targetName $REQUIRED_ARGUMENTS\n", KNRM))
      builder.append(wrapString(KWHT, s"--------------------------------------------------------------------------------------\n", KNRM))
      new ArgumentLookup(required:_*).ordered.foreach { arg =>
        ArgumentDefinitionPrinting.printArgumentDefinitionUsage(builder, arg, argumentLookup)
      }
    }

    if (optional.nonEmpty) {
      builder.append(wrapString(KRED, s"\n$targetName $OPTIONAL_ARGUMENTS\n", KNRM))
      builder.append(wrapString(KWHT, s"--------------------------------------------------------------------------------------\n", KNRM))
      new ArgumentLookup(optional:_*).ordered.foreach { argumentDefinition =>
        ArgumentDefinitionPrinting.printArgumentDefinitionUsage(builder, argumentDefinition, argumentLookup)
      }
    }

    builder.toString
  }

  /** Gets the command line assuming `parseTasks` has been called */
  def getCommandLine(printCommon: Boolean = true): String = {
    val argumentList = this.argumentLookup.ordered.filterNot(_.omitFromCommandLine)
    val toolName: String = targetName
    val commandLineString: StringBuilder = new StringBuilder

    commandLineString.append(argumentList.filter( _.hasValue).filter(!_.isSpecial).map(_.toCommandLineString).mkString(" "))
    commandLineString.append(argumentList.filter(!_.hasValue).filter(!_.isSpecial).map(_.toCommandLineString).mkString(" "))

    s"$toolName ${commandLineString.toString}"
  }

  /**
    * After command line has been parsed, make sure that all required arguments have values, and that
    * lists with minimum # of elements have sufficient.
    *
    * throws [[CommandLineException]] if arguments requirements are not satisfied.
    */
  private def assertArgumentsAreValid(args: ArgumentLookup) {
    try {
      args.view.foreach { argumentDefinition =>
        // TODO: move the inside of this loop to [[ArgumentDefintion]]
        val fullName: String = argumentDefinition.longName
        val mutextArgumentNames: StringBuilder = new StringBuilder

        // Validate mutex's
        //NB:  Make sure to remove duplicates
        mutextArgumentNames.append(
          argumentDefinition.mutuallyExclusive
            .toList
            .filter { mutexArgument =>
              val mutextArgumentDef: Option[ArgumentDefinition] = args.forField(mutexArgument)
              mutextArgumentDef.isDefined && mutextArgumentDef.get.setByUser
            }
            .map { args.forField(_).get }
            .distinct.sortBy(_.index)
            .map { _.longName}
            .mkString(", ")
        )
        if (argumentDefinition.setByUser && mutextArgumentNames.nonEmpty) {
          throw new UserException(s"Argument '$fullName' cannot be used in conjunction with argument(s) ${mutextArgumentNames.toString}")
        }

        if (argumentDefinition.isCollection && !argumentDefinition.optional) {
          argumentDefinition.validateCollection()
        }
        else if (!argumentDefinition.optional && !argumentDefinition.hasValue && mutextArgumentNames.isEmpty) {
          throw new UserException(getRequiredArgumentWithMutex(fullName, argumentDefinition))
        }
      }
    }
    catch {
      case e: IllegalAccessException =>
        throw new CommandLineParserInternalException("Should never happen", e)
    }
  }

  /**
    * Read an argument file and return a list of the args contained in it
    * A line that starts with [[CommandLineParserStrings.COMMENT]] is ignored.
    *
    * @param argumentsFile a text file containing args
    * @return the list of arguments from the file
    */
  private def loadArgumentsFile(argumentsFile: String): Traversable[String] = {
    CommandLineParser.parseArgumentsFile(Source.fromFile(argumentsFile).getLines.toTraversable)
  }

  /** Returns the implementation version string of the given class */
  def version: String = "Version:" + targetClass.getPackage.getImplementationVersion
}
