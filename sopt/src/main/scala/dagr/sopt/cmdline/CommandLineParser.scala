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

import dagr.commons.reflect.ReflectionUtil
import dagr.commons.util.StringUtil
import dagr.sopt.clp
import dagr.sopt.util.ParsingUtil._
import dagr.sopt.util._

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.{Set, mutable}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/** Various constants and methods for formatting and printing usages and error messages on the command line */
trait CommandLineParserStrings {

  /** Lengths for names and descriptions on the command line */
  val SubCommandGroupNameColumnLength = 48
  val SubCommandGroupDescriptionColumnLength = 45
  val SubCommandNameColumnLength = 45

  /** The maximum line lengths for tool descriptions */
  val MaximumLineLength = 80

  /** Error messages */
  val AvailableSubCommands = "Available Sub-Commands:"
  val MissingSubCommand = "No sub-command given."

  /** Section headers */
  val UsagePrefix = "USAGE:"
  val RequiredArguments = "Required Arguments:"
  val OptionalArguments = "Optional Arguments:"

  /** Section separator */
  val SeparatorLine = KWHT(s"--------------------------------------------------------------------------------------\n")

  /** Similarity floor for when searching for similar sub-command names. **/
  val HelpSimilarityFloor: Int = 7
  val MinimumSubstringLength: Int = 5

  /** The command line name. */
  def commandLineName: String

  /** Formats the short description of a tool when printing out the list of sub-commands in the usage.
    *
    * (1) find the first period (".") and keep only everything before it
    * (2) shorten it to the maximum line length adding "..." to the end.
    */
  def formatShortDescription(description: String): String = {
    val desc = description.stripMargin.dropWhile(_ == '\n')
    desc.indexOf('.') match {
      case -1 =>
        if (desc.length > MaximumLineLength-3) desc.substring(0, MaximumLineLength-3) + "..."
        else desc
      case idx => formatShortDescription(desc.substring(0, idx)) + "."
    }
  }

  /** Wraps an error string in terminal escape codes for display. */
  private[cmdline] def wrapError(s: String) : String = KERROR(s)

  /**
    * A typical command line program will call this to get the beginning of the usage message,
    * and then append a description of the program.
    */
  def standardSubCommandUsagePreamble(clazz: Option[Class[_]] = None): String = {
    standardCommandAndSubCommandUsagePreamble(commandClazz=None, subCommandClazz=clazz)
  }

  /** The name of "command" on the command line */
  def genericClpNameOnCommandLine: String = "command"

  /**
    * A typical command line program will call this to get the beginning of the usage message,
    * and then append a description of the program.
    */
  def standardCommandAndSubCommandUsagePreamble(commandClazz: Option[Class[_]] = None, subCommandClazz: Option[Class[_]] = None): String = {
    (commandClazz, subCommandClazz) match {
      case (Some(commandCz), Some(subCommandCz)) =>
        val mainName = commandLineName // commandCz.getSimpleName
        val clpName = subCommandCz.getSimpleName
        s"${KRED(UsagePrefix)} ${KBLDRED(mainName)} ${KRED(s"[$mainName arguments] [$genericClpNameOnCommandLine name] [$genericClpNameOnCommandLine arguments]")}" // FIXME: use clpName
      case (Some(commandCz), None) =>
        val mainName = commandLineName //mainCz.getSimpleName
        s"${KRED(UsagePrefix)} ${KBLDRED(mainName)} ${KRED(s"[$mainName arguments] [$genericClpNameOnCommandLine name] [$genericClpNameOnCommandLine arguments]")}"
      case (None, Some(subCommandCz)) =>
        val clpName = subCommandCz.getSimpleName
        s"${KRED(UsagePrefix)} ${KBLDRED(clpName)} ${KRED(s"[arguments]")}"
      case (None, None) =>
        s"${KRED(UsagePrefix)} ${KBLDRED(commandLineName)} ${KRED(s"[$genericClpNameOnCommandLine] [arguments]")}"
    }
  }

  /** Returns the implementation version string of the given class */
  protected def version(clazz: Class[_]): String = "Version:" + clazz.getPackage.getImplementationVersion
}

/** Class for parsing the command line when we also have sub-commands.
  *
  * We have two possible entry points:
  * (1) [[parseSubCommand()]] for when we have set of sub-commands, each having their own set of arguments, and we
  *     use the first argument to get the class's simple name (`getSimpleName`), and pass the rest of the arguments
  *     to a properly instantiated [[CommandLineProgramParser]] to parse the arguments and build an instance of that
  *     class.
  * (2) [[parseCommandAndSubCommand]] for the same case as (1), but when the main program itself also has arguments, which
  *     come prior to the name of the command line program.
  *
  * To route the arguments to build the appropriate program, the parser searches for a command line program that is a
  * sub-class of [[SubCommand]], and creates a [[CommandLineProgramParser]] to parse the args and return an instance of the
  * [[SubCommand]].
  *
  * Constructor arguments for the sub-command classes or command class can be annotated with [[dagr.sopt.arg]] while
  * constructors themselves can be annotated with [[dagr.sopt.clp]].  The latter may be omitted, but is extremely
  * useful for grouping related sub-commands and having a common description.
  *
  * @param commandLineName the name of the base command line.
  * @param classTag the [[ClassTag]] of [[SubCommand]] , usually inferred.
  * @param typeTag the [[TypeTag]] of [[SubCommand]] , usually inferred.
  * @tparam SubCommand the base class for all sub-commands to display and parse on the command line.
  */
class CommandLineParser[SubCommand](val commandLineName: String)
                                   (implicit classTag: ClassTag[SubCommand], typeTag: TypeTag[SubCommand])
  extends CommandLineParserStrings {

  private type SubCommandClass = Class[_ <: SubCommand]
  private type SubCommandGroupClass = Class[_ <: ClpGroup]

  /** The error message for an unknown sub-command. */
  private[cmdline] def unknownSubCommandErrorMessage(command: String, classes: Set[SubCommandClass] = Set.empty): String = {
    s"'$command' is not a valid sub-command. See $commandLineName --help for more information." + printUnknown(command, classes.map(_.getSimpleName))
  }

  /**
    * Finds the class of a sub-command using the first element in the args array.
    *
    * @param args the arguments, with the first argument used as the sub-command name.
    * @param classes the set of available sub-command classes.
    * @return the class of the sub-commands or an error string if either no arguments were given or no
    *         sub-command class matched the given argument.
    */
  private def parseSubCommandName(args: Array[String], classes: Set[SubCommandClass]): Either[SubCommandClass,String] = {
    if (args.length < 1) {
      Right(MissingSubCommand)
    }
    else {
      val clazzOption: Option[SubCommandClass] = classes.find(clazz => 0 == args(0).compareTo(clazz.getSimpleName))
      clazzOption match {
        case Some(clazz) => Left(clazz)
        case None        => Right(unknownSubCommandErrorMessage(args(0), classes))
      }
    }
  }


  /** Returns the implementation version string of the given class */
  protected def version: String = version(getClass)

  /**
    * Prints the top-level usage of the command line, with a standard pre-amble, version, and list of sub-commands available.
    *
    * @param classes the classes corresponding to the sub-commands.
    * @param commandLineName the name of this sub-command.
    */
  private[cmdline] def subCommandListUsage(classes: Set[SubCommandClass], commandLineName: String, withPreamble: Boolean): String = {
    val builder = new StringBuilder

    if (withPreamble) {
      builder.append(s"${standardSubCommandUsagePreamble()}\n")
      builder.append(KRED(s"$version\n"))
    }

    builder.append(KBLDRED(s"$AvailableSubCommands\n"))
    val subCommandsGroupClassToClpGroupInstance: mutable.Map[SubCommandGroupClass, ClpGroup] = new mutable.HashMap[SubCommandGroupClass, ClpGroup]
    val subCommandsByGroup: java.util.Map[ClpGroup, ListBuffer[SubCommandClass]] = new java.util.TreeMap[ClpGroup, ListBuffer[SubCommandClass]]()
    val subCommandsToClpAnnotation: mutable.Map[SubCommandClass, clp] = new mutable.HashMap[SubCommandClass, clp]

    classes.foreach { clazz =>
      findClpAnnotation(clazz) match {
        case None => throw new BadAnnotationException(s"The class '${clazz.getSimpleName}' is missing the required @clp annotation.")
        case Some(clp) =>
          subCommandsToClpAnnotation.put(clazz, clp)
          val clpGroup: ClpGroup = subCommandsGroupClassToClpGroupInstance.get(clp.group) match {
            case Some(group) => group
            case None =>
              val group: ClpGroup = clp.group.newInstance
              subCommandsGroupClassToClpGroupInstance.put(clp.group, group)
              group
          }
          Option(subCommandsByGroup.get(clpGroup)) match {
            case Some(clps) =>
              clps.add(clazz)
            case None =>
              val clps = ListBuffer[SubCommandClass](clazz)
              subCommandsByGroup.put(clpGroup, clps)
          }
      }
    }

    subCommandsByGroup.entrySet.foreach{entry =>
      val clpGroup: ClpGroup = entry.getKey
      builder.append(SeparatorLine)
      builder.append(KRED(String.format(s"%-${SubCommandGroupNameColumnLength}s%-${SubCommandGroupDescriptionColumnLength}s\n",
        clpGroup.name + ":", clpGroup.description)))
      val entries: List[SubCommandClass] = entry.getValue.toList
      entries
        .sortWith((lhs, rhs) => lhs.getSimpleName.compareTo(rhs.getSimpleName) < 0)
        .foreach{clazz =>
          val clp: clp = subCommandsToClpAnnotation.get(clazz).get
          builder.append(KGRN(String.format(s"    %-${SubCommandNameColumnLength}s%s\n",
            clazz.getSimpleName, KCYN(formatShortDescription(clp.description)))))
        }
    }

    builder.append(SeparatorLine)
    builder.toString()
  }

  /** The main method for parsing command line arguments.  Typically, the arguments are the format
    * `[clp Name] [clp arguments]`.  This in contrast to [[parseCommandAndSubCommand]], which includes arguments to the main class in
    * the `[main arguments]` section.
    *
    *   (1) Finds all classes that extend [[SubCommand]] to include them as valid command line programs on the command line.
    *   (2) When the first arg is one of the command line programs names (usually `getSimpleName`), it creates a
    *       [[CommandLineProgramParser]] (SubCommand parser) for the given command line program, and allows the SubCommand parser to
    *       parse the rest of the args.
    *   (3) Returns `None` in the case of any parsing error, or an instance of the command line program constructed
    *       according to the parsed arguments.
    *
    * @param args              the command line arguments to parse.
    * @param packageList       the list of packages to search for classes that extend [[SubCommand]].
    * @param omitSubClassesOf  individual classes to omit from including on the command line.
    * @param includeHidden     true to include classes whose [[clp]] annotation's hidden values is set to false.
    * @param afterSubCommandBuild  a block of code to execute after a [[clp]] instance has been created successfully.
    * @param extraUsage        an optional string that will be printed prior to any usage message.
    * @return                  a [[clp]] instance initialized with the command line arguments, or [[None]] if
    *                          unsuccessful.
    */
  def parseSubCommand(args: Array[String],
                      packageList: List[String],
                      omitSubClassesOf: Iterable[Class[_]] = Nil,
                      includeHidden: Boolean = false,
                      afterSubCommandBuild: SubCommand => Unit = c => Unit,
                      extraUsage: Option[String] = None): Option[SubCommand] = {

    /** A brief developer note about the precedence of printing error messages and usages.
      *
      * See [[parseCommandAndSubCommand]] for 1-3, for when we have options to the main tool.
      *
      * 4. Complain if (a) no clp name is given, or (b) it is not recognized.
      * 5. Complain if clp options are mis-specified.
      * 6. Print the clp help message if --help is used after a clp name.
      * 7. Print the version if --version is used after a clp name.
      * 8. ... execute the clp ...
      * */

    val print : (String => Unit) = System.err.println

    def printExtraUsage(clazz: Option[Class[_]]): Unit = {
      extraUsage match {
        case Some(str) => print(str)
        case None => print(standardSubCommandUsagePreamble(clazz))
      }
    }

    // Try parsing the task name
    val classToClpAnnotation = findClpClasses[SubCommand](packageList, omitSubClassesOf = omitSubClassesOf, includeHidden = includeHidden)
    parseSubCommandName(args = args, classes = classToClpAnnotation.keySet) match {
      case Right(error) => // Case 4 (b)
        printExtraUsage(None)
        print(subCommandListUsage(classToClpAnnotation.keySet, commandLineName, withPreamble=false))
        print(wrapError(error))
        None
      case Left(clazz) =>
        /////////////////////////////////////////////////////////////////////////
        // Parse the arguments for the Command Line Program class
        /////////////////////////////////////////////////////////////////////////
        val clpParser = new CommandLineProgramParser(clazz)
        clpParser.parseAndBuild(args.drop(1)) match {
          case ParseFailure(ex, _) => // Case 5
            printExtraUsage(clazz=Some(clazz))
            print(clpParser.usage(withPreamble=false))
            print(wrapError(ex.getMessage))
            None
          case ParseHelp() => // Case 6
            printExtraUsage(clazz=Some(clazz))
            print(clpParser.usage(withPreamble=false))
            None
          case ParseVersion()  => // Case 7
            print(clpParser.version)
            None
          case ParseSuccess() => // Case 8
            val clp: SubCommand = clpParser.instance.get
            try {
              afterSubCommandBuild(clp)
              Some(clp)
            }
            catch {
              case ex: ValidationException =>
                printExtraUsage(clazz=Some(clazz))
                print(clpParser.usage(withPreamble=false))
                ex.messages.foreach(msg => print(wrapError(msg)))
                None
            }
        }
    }
  }

  /** Method for parsing command arguments as well as a specific sub-command arguments.
    * Typically, the arguments are the format `[command arguments] [sub-command] [sub-command arguments]`.  This in
    * contrast to [[parseSubCommand]], which omits the `[command arguments]` section.
    *
    *   (1) Searches for `--` or a recognized [[SubCommand]] in the list of arguments to partition the main and clp arguments. The
    *       [[SubCommand]]s are found by searching for all classes that extend [[SubCommand]].
    *   (2) Parses the main arguments and creates an instance of main, and returns None if any error occurred.
    *   (3) Executes the `afterMainBuilding` of code after the main instance has been created.
    *   (3) Parses the clp arguments and creates an instance of clp, and returns None if any error occurred.
    *   (3) Executes the `afterClpBuilding` of code after the clp instance has been created.
    *
    * It creates [[CommandLineProgramParser]]s to parse the main class and clp arguments respectively.
    *
    * @param args              the command line arguments to parse.
    * @param packageList       the list of packages to search for classes that extend [[SubCommand]].
    * @param omitSubClassesOf  individual classes to omit from including on the command line.
    * @param includeHidden     true to include classes whose [[clp]] annotation's hidden values is set to false.
    * @param afterCommandBuild a block of code to execute after a [[CommandClass]] (main) instance has been created successfully.
    * @param afterSubCommandBuild a block of code to execute after a [[clp]] instance has been created successfully.
    * @return                  a [[clp]] instance initialized with the command line arguments, or [[None]] if
    *                          unsuccessful.
    */
  def parseCommandAndSubCommand[CommandClass](args: Array[String],
                                           packageList: List[String],
                                           omitSubClassesOf: Iterable[Class[_]] = Nil,
                                           includeHidden: Boolean = false,
                                           afterCommandBuild: CommandClass => Unit = (t: CommandClass) => Unit,
                                           afterSubCommandBuild: CommandClass => SubCommand => Unit = (t: CommandClass) => (c: SubCommand) => Unit)
                                          (implicit tt: TypeTag[CommandClass]): Option[(CommandClass, SubCommand)] = {
    val mainClazz: Class[CommandClass] = ReflectionUtil.typeTagToClass[CommandClass]
    val clpClazz: Class[SubCommand] = ReflectionUtil.typeTagToClass[SubCommand]
    val thisParser = this

    // Parse the args for the main class
    val mainClassParser = new CommandLineProgramParser(mainClazz) {
      override protected def standardUsagePreamble: String = {
        standardCommandAndSubCommandUsagePreamble(Some(mainClazz), None) + "\n"
      }
      override protected def targetName: String = thisParser.commandLineName
      override def commandLineName: String = thisParser.commandLineName
      override def genericClpNameOnCommandLine: String = thisParser.genericClpNameOnCommandLine
    }

    val print : (String => Unit) = System.err.println
    val (mainClassArgs, clpArgs) = splitArgs(args, packageList, Seq(mainClazz), includeHidden = includeHidden)

    /** A brief developer note about the precedence of printing error messages and usages.
      *
      * 1. Complain if (a) command options are mis-specified, or (b) the clp name was not recognized (must be the last
      *    argument in the `mainClassArgs` and have no leading dash).
      * 2. Print the commnd-only help message if --help is used before a clp name.
      * 3. Print the version if --version is used before a clp name.
      * 4. Complain if (a) no clp name is given, or (b) it is not recognized.
      * 5. Complain if clp options are mis-specified.
      * 6. Print the command-and-sub-command help message if --help is used after a clp name.
      * 7. Print the version if --version is used after a clp name.
      * 8. ... execute the sub-command ...
      * */

    /////////////////////////////////////////////////////////////////////////
    // Try parsing and building CommandClass and handle the outcomes
    /////////////////////////////////////////////////////////////////////////
    mainClassParser.parseAndBuild(args=mainClassArgs) match {
      case ParseFailure(ex, remaining) => // Case (1)
        print(mainClassParser.usage())
        print(wrapError(ex.getMessage))
        None
      case ParseHelp() => // Case (2)
        val classes = findClpClasses[SubCommand](packageList, omitSubClassesOf = Seq(mainClazz), includeHidden = includeHidden).keySet
        print(mainClassParser.usage())
        print(subCommandListUsage(classes, commandLineName, withPreamble=true))
        None
      case ParseVersion() => // Case (3)
        print(mainClassParser.version)
        None
      case ParseSuccess() => // Case (4-8)
        /////////////////////////////////////////////////////////////////////////
        // Get setup, and attempt to ID and load the clp class
        /////////////////////////////////////////////////////////////////////////
        val mainInstance = mainClassParser.instance.get
        afterCommandBuild(mainInstance)

        val clpInstance = this.parseSubCommand(
          args             = clpArgs,
          packageList      = packageList,
          omitSubClassesOf = Seq(mainClazz),
          includeHidden    = includeHidden,
          afterSubCommandBuild         = afterSubCommandBuild(mainInstance),
          extraUsage       = Some(mainClassParser.usage())
        )
        clpInstance match {
          case Some(p) => Option(mainInstance, p)
          case None => None
        }
    }
  }

  /** Splits the given args into two Arrays, first splitting based on a "--", and if not found,
    * searching for a program name.  The "--" arg will not be returned.
    */
  private[cmdline] def splitArgs(args: Array[String],
                                 packageList: List[String],
                                 omitSubClassesOf: Iterable[Class[_]] = Nil,
                                 includeHidden: Boolean = false) : (Array[String], Array[String]) = {
    if (args.isEmpty) return (args, Array[String]())

    val subCommands = findClpClasses[SubCommand](packageList = packageList, omitSubClassesOf = omitSubClassesOf, includeHidden=includeHidden).keys

    // first check for "--"
    args.indexOf("--") match {
      case -1 =>
        // check for an exact match
        subCommands.view.map { p => args.indexOf(p.getSimpleName) }.filter(_ != -1).reduceOption(_ min _) match {
          case Some(n) => args.splitAt(n)
          case None => // args must be non-empty
            // try finding the most similar arg and pipeline name
            val distances = args.toList.map { arg =>
              if (arg.startsWith("-")) Integer.MAX_VALUE // ignore obvious options
              else findSmallestSimilarityDistance(arg, subCommands.map(_.getSimpleName))
            }
            // if we found one that was similar, then split the args at that point
            distances.zipWithIndex.min match {
              case (distance, idx) if distance < Integer.MAX_VALUE => args.splitAt(idx)
              case (distance, idx) => (args, Array[String]())
            }
        }
      case idx =>
        (args.take(idx), args.drop(idx+1))
    }
  }
}

