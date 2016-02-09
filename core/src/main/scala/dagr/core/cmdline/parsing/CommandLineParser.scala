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

import dagr.core.cmdline._
import dagr.core.cmdline.parsing.ParseResult.ParseResult
import dagr.core.tasksystem.ValidationException
import dagr.core.util.LazyLogging
import dagr.core.util.StringUtil._
import dagr.sopt.{OptionParser, OptionParsingException}

import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success}

/** Stores strings used in the usage and error messages */
private object CommandLineParserStrings {
  val RequiredArguments = "Required Arguments:"
  val OptionalArguments = "Optional Arguments:"
  val UsagePrefix = "USAGE:"
  val Comment = "#"

  def getRequiredArgument(fullName: String): String = {
    s"Argument '$fullName' is required"
  }

  def getRequiredArgumentWithMutex(fullName: String, arg: ClpArgument): String = {
    s"${getRequiredArgument(fullName)}" +
      (if (arg.mutuallyExclusive.isEmpty) "." else s" unless any of ${arg.mutuallyExclusive} are specified.")
  }
}

/** Enumeration that represents the possible outcomes of trying to parse a command line. */
private[parsing] object ParseResult extends Enumeration {
  type ParseResult = Value
  val Success = Value // Successful parse
  val Failure = Value // Something went wrong (unknown option, missing arg, etc.)
  val Help    = Value // A require for command line help was encountered
  val Version = Value // A request to display version was encountered
}

private[parsing] object CommandLineParser {
  def parseArgumentsFile(lines: Traversable[String]): Traversable[String] = {
    val args: mutable.ListBuffer[String] = new mutable.ListBuffer[String]
    lines.foreach { line =>
      if (!line.startsWith(CommandLineParserStrings.Comment) && line.trim.nonEmpty) {
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
private[parsing] class CommandLineParser[T](val targetClass: Class[T]) extends LazyLogging {
  import CommandLineParserStrings._
  import ParsingUtil._

  val argumentLookup: ClpArgumentLookup = new ClpArgumentLookup()
  //private val argumentsFilesLoadedAlready: mutable.Set[String] = new mutable.HashSet[String]

  /** The instance of type T that is None initially, and will be populated with a T after a successful parse(). */
  var instance : Option[T] = None
  /** The special args object that is None initially, and will be populated with a value after a successful parse(). */
  var specialArguments : Option[SpecialArgumentsCollection] = None

  // Actual constructor code here!
  createArgumentDefinitions(classOf[SpecialArgumentsCollection], this.argumentLookup)
  createArgumentDefinitions(targetClass, this.argumentLookup)

  protected def targetName: String = targetClass.getSimpleName

  private[parsing] def setMutexArguments(lookup: ClpArgumentLookup): Unit = {
    // set mutex arguments
    lookup.ordered.filterNot(_.hidden).foreach { argumentDefinition: ClpArgument =>
      val argumentAnnotation = argumentDefinition.annotation
      val sourceFieldName = argumentDefinition.name
      argumentAnnotation.get.mutex.foreach { targetFieldName =>
        val mutexArgumentDef: Option[ClpArgument] = lookup.forField(targetFieldName)
        if (mutexArgumentDef.isDefined) {
          mutexArgumentDef.get.mutuallyExclusive.add(sourceFieldName)
        }
        else {
          throw new BadAnnotationException(s"Bad Arg annotation in field '$sourceFieldName': " +
              s"mutex field '$targetFieldName' was not found in the list of fields."
          )
        }
      }
    }
  }

  /**
    * Creates argument definitions for all the fields in the provided class, and stores them in the
    * provided ArgumentLookup.
    */
  private[parsing] def createArgumentDefinitions(declaringClass: Class[_], lookup: ClpArgumentLookup): Unit = {
    new ClpReflectiveBuilder(declaringClass).argumentLookup.view.foreach(lookup.add)
    setMutexArguments(lookup=lookup)
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
      ParseResult.Failure
    }
    else { // Then build the instances
      try {
        val (specials, normals) = this.argumentLookup.ordered.partition(p => p.declaringClass == classOf[SpecialArgumentsCollection])
        val specialHelper = new ClpReflectiveBuilder(classOf[SpecialArgumentsCollection])
        this.specialArguments = Some(specialHelper.build(specials.map(d => d.value.get)))
        this.specialArguments.find(special => special.version || special.help) match {
          case Some(special) if special.version => ParseResult.Version
          case Some(special) if special.help => ParseResult.Help
          case _ => // neither version nor help
            assertArgumentsAreValid(this.argumentLookup)
            val normalHelper = new ClpReflectiveBuilder(targetClass)
            this.instance = Some(normalHelper.build(normals.map(d => d.value.orNull)))
            ParseResult.Success
        }
      }
      catch {
        case ValidationException(xs) =>
          xs foreach { errorMessageBuilder.append("\n").append(_).append("\n") }
          ParseResult.Failure
        case e: Exception =>
          errorMessageBuilder.append(s"\n${e.getClass.getSimpleName}: ${e.getMessage}\n")
          ParseResult.Failure
      }
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
      this.argumentLookup.ordered.filterNot(_.hidden).foreach {
        case arg if arg.isFlag =>        parser.acceptFlag(          arg.names: _*)
        case arg if !arg.isCollection => parser.acceptSingleValue(   arg.names: _*)
        case arg =>                      parser.acceptMultipleValues(arg.names: _*)
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
          this.argumentLookup.forArg(name).get.setArgument(values:_*)
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
  protected def getStandardUsagePreamble: String = s"$KRED$UsagePrefix $KBLDRED" + targetName + s"$KNRM$KRED [arguments]$KNRM\n\n"

  /**
    * Print a usage message for a given command line task.
    *
    * @param printCommon True if common args should be included in the usage message.
    */
  def usage(printCommon: Boolean = true, withPreamble: Boolean = true, withSpecial: Boolean = true): String = {
    val builder = new StringBuilder()

    if (withPreamble) {
      builder.append(s"$getStandardUsagePreamble")
      findClpAnnotation(targetClass) match {
        case Some(anno) => builder.append(wrapString(KCYN, s"${formatLongDescription(anno.description())}", KNRM))
        case None =>
      }
      builder.append(wrapString(KRED, s"$version\n", KNRM))
    }

    // filter on common and partition on optional
    val (required, optional) = argumentLookup.view
      .filterNot { _.hidden }
      .filter { printCommon || !_.isCommon }
      .filter { withSpecial || !_.isSpecial }
      .partition { !_.optional }

    if (required.nonEmpty) {
      builder.append(wrapString(KRED, s"\n$KBLDRED$targetName$KNRM$KRED $RequiredArguments\n", KNRM))
      builder.append(wrapString(KWHT, s"--------------------------------------------------------------------------------------\n", KNRM))
      new ClpArgumentLookup(required:_*).ordered.foreach { arg =>
        ClpArgumentDefinitionPrinting.printArgumentDefinitionUsage(builder, arg, argumentLookup)
      }
    }

    if (optional.nonEmpty) {
      builder.append(wrapString(KRED, s"\n$KBLDRED$targetName$KNRM$KRED $OptionalArguments\n", KNRM))
      builder.append(wrapString(KWHT, s"--------------------------------------------------------------------------------------\n", KNRM))
      new ClpArgumentLookup(optional:_*).ordered.foreach { argumentDefinition =>
        ClpArgumentDefinitionPrinting.printArgumentDefinitionUsage(builder, argumentDefinition, argumentLookup)
      }
    }

    builder.toString
  }

  /** add extra whitespace after newlines for pipeline descriptions*/
  private def formatLongDescription(description: String): String = {
    val desc = description.stripMargin
    desc
      .dropWhile(_ == '\n')
      .dropRight(1) + (if (desc.isEmpty) "" else desc.last) + "\n"
  }

  /** Gets the command line assuming `parseTasks` has been called */
  def getCommandLine(printCommon: Boolean = true): String = {
    val argumentList = this.argumentLookup.ordered.filterNot(_.hidden)
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
  private def assertArgumentsAreValid(args: ClpArgumentLookup) {
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
              val mutextArgumentDef: Option[ClpArgument] = args.forField(mutexArgument)
              mutextArgumentDef.isDefined && mutextArgumentDef.get.isSetByUser
            }
            .map { args.forField(_).get }
            .distinct.sortBy(_.index)
            .map { _.longName}
            .mkString(", ")
        )
        if (argumentDefinition.isSetByUser && mutextArgumentNames.nonEmpty) {
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
    * A line that starts with [[CommandLineParserStrings.Comment]] is ignored.
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
