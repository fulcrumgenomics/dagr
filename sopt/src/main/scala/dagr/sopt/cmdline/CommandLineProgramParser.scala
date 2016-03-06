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

import java.io.IOException

import dagr.commons.util.LazyLogging
import dagr.commons.CommonsDef._
import dagr.sopt.parsing.{OptionParsingException, OptionParser}
import dagr.sopt.util.ParsingUtil._
import dagr.sopt.util._

import scala.util.{Try, Failure, Success}

/** Stores strings used in the usage and error messages */
object CommandLineProgramParserStrings {

  val Comment = "#"
  val CannotLoadArgumentFilesMessage = "Couldn't load arguments file: "

  def requiredArgumentErrorMessage(fullName: String): String = {
    s"Argument '$fullName' is required"
  }

  def requiredArgumentWithMutexErrorMessage(fullName: String, arg: ClpArgument): String = {
    s"${requiredArgumentErrorMessage(fullName)}" +
      (if (arg.mutuallyExclusive.isEmpty) "." else s" unless any of ${arg.mutuallyExclusive} are specified.")
  }

  /** Returns the implementation version string of the given class */
  def version(clazz: Class[_], color: Boolean = true): String = {
    val v = "Version: " + clazz.getPackage.getImplementationVersion
    if (color) KRED(v) else v
  }

  def lineBreak(color: Boolean = true): String = {
    val v = s"--------------------------------------------------------------------------------------\n"
    if (color) KWHT(v) else v
  }
}

object CommandLineProgramParser {

  /** Attempts to build an instance of the target class from the given arguments.
    *
    * It will print a usage message if the help argument is given, or print the version if the version argument is given.
    * If an error is encountered, it will print the usage message as well as the error message.
    *
    * @param targetClass the class with annotated constructor parameters.
    * @param args the arguments.
    * @tparam T the type of the target class, usually inferred.
    * @return None if not successful, otherwise an instance of [[T]].
    */
  def parseAndBuild[T](targetClass: Class[T], args: Array[String]): Option[T] = {
    val print : (String => Unit) = System.err.println
    val parser = new CommandLineProgramParser(targetClass=targetClass)

    parser.parseAndBuild(args=args) match {
      case ParseSuccess() =>
      case ParseHelp() => print(parser.usage())
      case ParseVersion() => print(parser.version)
      case ParseFailure(ex, remaining) => print(s"${parser.usage()}\n${parser.wrapError(ex.getMessage)}")
    }
    parser.instance
  }
}

/** Class for parsing the command line for a single command line program.
  *
  * The main entry point is [[parseAndBuild()]].  The arguments given to this method should match the annotated
  * arguments to the program's constructor.  Constructor arguments can be annotated with [[dagr.sopt.arg]] while
  * constructors themselves can be annotated with [[dagr.sopt.clp]].  The latter may be omitted, but is useful for
  * grouping related command line programs and having a common description.
  */
class CommandLineProgramParser[T](val targetClass: Class[T]) extends CommandLineParserStrings with LazyLogging {
  import CommandLineProgramParserStrings._

  override def commandLineName: String = targetClass.getSimpleName

  val argumentLookup: ClpArgumentLookup = new ClpArgumentLookup()
  //private val argumentsFilesLoadedAlready: mutable.Set[String] = new mutable.HashSet[String]

  /** The instance of type T that is None initially, and will be populated with a T after a successful parse(). */
  var instance : Option[T] = None

  /** The special args object that is None initially, and will be populated with a value after a successful parse(). */
  private var specialArguments : Option[SpecialArgumentsCollection] = None

  // Actual constructor code here!
  createArgumentDefinitions(classOf[SpecialArgumentsCollection], this.argumentLookup)
  createArgumentDefinitions(targetClass, this.argumentLookup)

  protected def targetName: String = targetClass.getSimpleName

  private[cmdline] def setMutexArguments(lookup: ClpArgumentLookup): Unit = {
    // set mutex arguments
    lookup.ordered.filterNot(_.hidden).foreach { argumentDefinition: ClpArgument =>
      val argumentAnnotation = argumentDefinition.annotation
      val sourceFieldName = argumentDefinition.name
      argumentAnnotation.get.mutex.foreach { targetFieldName =>
        lookup.forField(targetFieldName) match {
          case Some(mutexArgumentDef) => mutexArgumentDef.mutuallyExclusive.add(sourceFieldName)
          case None =>
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
  private[cmdline] def createArgumentDefinitions(declaringClass: Class[_], lookup: ClpArgumentLookup): Unit = {
    val clpReflectiveBuilder = Try { new ClpReflectiveBuilder(declaringClass)} match {
      case Success(builder) => builder
      case Failure(ex: IllegalStateException) => throw ex
      case Failure(thr) => throw new CommandLineException(thr.getMessage + "\n" + thr.getStackTrace.mkString("\n"))
    }
    clpReflectiveBuilder.argumentLookup.view.foreach(lookup.add)
    setMutexArguments(lookup=lookup)
  }

  /**
    * Parses the command line arguments for a given class and attempts to instantiate the class.
    * Appends any errors to the `stringBuilder` if parsing is unsuccessful.
    *
    * @param args the args to parse.
    * @return an instance of the class [[T]] if we successfully parsed the args, false otherwise
    */
  def parseAndBuild(args: Array[String]): ParseResult = {
    // Try parsing the arguments
    parseArgs(args=args) match {
      case ParseFailure(ex, remaining) => ParseFailure(ex, remaining)
      case ParseSuccess() => // Then build the instances
        try {
          val (specials, normals) = this.argumentLookup.ordered.partition(p => p.declaringClass == classOf[SpecialArgumentsCollection])
          val specialHelper = new ClpReflectiveBuilder(classOf[SpecialArgumentsCollection])
          this.specialArguments = Some(specialHelper.build(specials.map(d => d.value.get)))
          this.specialArguments.find(special => special.version || special.help) match {
            case Some(sp) if sp.version => ParseVersion()
            case Some(sp) if sp.help => ParseHelp()
            case _ => // neither version nor help
              assertArgumentsAreValid(this.argumentLookup)
              val normalHelper = new ClpReflectiveBuilder(targetClass)
              this.instance = Some(normalHelper.build(normals.map(d => d.value.orNull)))
              ParseSuccess()
          }
        }
        catch {
          case ValidationException(xs) => ParseFailure(ex=new CommandLineException(xs.mkString("\n")), remaining=Nil)
          case e: Exception => ParseFailure(ex=new CommandLineException(s"${e.getClass.getSimpleName}: ${e.getMessage}"), remaining=Nil)
        }
      case ParseVersion() | ParseHelp() => throw new IllegalStateException("BUG: ParseVersion or ParseHelp was not expected")
    }
  }

  /**
    * Attempts to parse the command line recursively, by parsing the arguments in the args array, loading any arguments files
    * indicated, and then repeating again for each file.
    */
  private def parseArgs(args: Array[String]): ParseResult = {
    val parser: OptionParser = new OptionParser(argFilePrefix=Some("@"))

    try {
      // Add to the option parsers
      this.argumentLookup.ordered.filterNot(_.hidden).foreach {
        case arg if arg.isFlag =>        parser.acceptFlag(          arg.names: _*)
        case arg if !arg.isCollection => parser.acceptSingleValue(   arg.names: _*)
        case arg =>                      parser.acceptMultipleValues(arg.names: _*)
      }

      // Parse the args
      parser.parse(args.toList) match {
        case Success(_) =>
          // set the values
          val parseResults: Traversable[ParseResult] = parser.map {
            case (name: String, values: List[String]) =>
              this.argumentLookup.forArg(name).foreach(arg => arg.setArgument(values:_*))
              ParseSuccess()
            case _ =>
              ParseFailure(ex=new IllegalStateException("Parser returned an unexpected set of values"), parser.remaining)
          }
          // find any failure and return the first one, otherwise return a success
          parseResults.find {
            case ParseFailure(ex, remaining) => true
            case _ => false
          }.getOrElse(ParseSuccess())
        case Failure(thr) => throw thr // this will be caught and routed appropriately below
      }
    }
    catch {
      case ex: OptionParsingException => ParseFailure(ex=ex, remaining=parser.remaining)
      case ex: CommandLineException   => ParseFailure(ex=ex, remaining=parser.remaining)
      case ex: IOException            => ParseFailure(ex=new CommandLineException(CannotLoadArgumentFilesMessage, ex), remaining=parser.remaining)
      case ex: Exception              => ParseFailure(ex=new CommandLineException("Unknown exception:", ex), remaining=parser.remaining)
    }
  }


  /**
    * A typical command line program will call this to get the beginning of the usage message,
    * and then append a description of the program, like this:
    */
  protected def standardUsagePreamble: String = s"${KRED(UsagePrefix)} ${KBLDRED(targetName)}${KRED(" [arguments]")}"

  /**
    * Print a usage message for a given command line task.
    */
  def usage(printCommon: Boolean = true, withVersion: Boolean = true, withSpecial: Boolean = true): String = {
    val builder = new StringBuilder()

    builder.append(s"$standardUsagePreamble")
    if (withVersion) {
      builder.append(s"\n$version")
    }
    builder.append("\n" + lineBreak(color=true))
    findClpAnnotation(targetClass) match {
      case Some(anno) => builder.append(KCYN(s"${formatLongDescription(anno.description())}\n"))
      case None =>
    }

    // filter on common and partition on optional
    val (required, optional) = argumentLookup.view
      .filterNot { _.hidden }
      .filter { printCommon || !_.isCommon }
      .filter { withSpecial || !_.isSpecial }
      .partition { !_.optional }

    if (required.nonEmpty) {
      builder.append(KRED(s"\n${KBLDRED(targetName)} ${KRED(RequiredArguments)}\n"))
      builder.append(lineBreak(color=true))
      new ClpArgumentLookup(required:_*).ordered.foreach { arg =>
        ClpArgumentDefinitionPrinting.printArgumentDefinitionUsage(builder, arg, argumentLookup)
      }
    }

    if (optional.nonEmpty) {
      builder.append(KRED(s"\n${KBLDRED(targetName)} ${KRED(OptionalArguments)}\n"))
      builder.append(lineBreak(color=true))
      new ClpArgumentLookup(optional:_*).ordered.foreach { argumentDefinition =>
        ClpArgumentDefinitionPrinting.printArgumentDefinitionUsage(builder, argumentDefinition, argumentLookup)
      }
    }

    builder.toString
  }

  /** add extra whitespace after newlines for pipeline descriptions*/
  private def formatLongDescription(description: String): String = {
    val desc = description.stripMargin.trim()
    desc
      .dropWhile(_ == '\n')
      .dropRight(1) + desc.lastOption.getOrElse("")
  }

  /** Gets the command line assuming `parseTasks` has been called */
  def commandLine(printCommon: Boolean = true): String = {
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
  private def assertArgumentsAreValid(args: ClpArgumentLookup): Unit = {
    try {
      args.view.foreach { argumentDefinition =>
        val fullName: String = argumentDefinition.longName
        val mutextArgumentNames: StringBuilder = new StringBuilder

        // Validate mutex's
        //NB:  Make sure to remove duplicates
        mutextArgumentNames.append(
          argumentDefinition.mutuallyExclusive
            .toList.flatMap(args.forField(_) match {
              case Some(m) if m.isSetByUser => Some(m)
              case _ => None
            }) // gets rid of the None
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
          throw new UserException(requiredArgumentWithMutexErrorMessage(fullName, argumentDefinition))
        }
      }
    }
    catch {
      case e: IllegalAccessException => unreachable("Should never happen: " + e.getMessage)
    }
  }

  def version: String = CommandLineProgramParserStrings.version(targetClass, color=true)
}
