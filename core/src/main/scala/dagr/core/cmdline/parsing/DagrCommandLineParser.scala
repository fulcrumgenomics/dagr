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

import java.nio.file.{Files, Paths}

import dagr.core.cmdline._
import dagr.core.config.Configuration
import dagr.core.tasksystem.{ValidationException, Pipeline}
import dagr.core.util.{PathUtil, StringUtil}
import dagr.core.util.StringUtil._

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.{Map, Set, mutable}

/** Stores strings used in the usage and error messages */
private[parsing] object DagrCommandLineParserStrings {
  val AVAILABLE_PIPELINES = "Available Pipelines:"

  def getUnknownPipeline(commandLineName: String): String = {
    s"No known pipeline name found.  If using --scripts, try using '--' separator between $commandLineName options and Pipeline name."
  }

  val MISSING_PIPELINE_NAME = "No pipeline name given."

  def getUnknownCommand(command: String): String = {
    s"'$command' is not a valid command. See DagrCommandLine --help for more information."
  }
}

/** Methods for parsing among top level command line tasks */
private[cmdline] class DagrCommandLineParser(val commandLineName: String, val includeHidden: Boolean = false) {
  import ParsingUtil._
  import CommandLineParserStrings._
  import DagrCommandLineParserStrings._

  /** Wraps an error string in terminal escape codes for display. */
  private def wrapError(s: String) : String = StringUtil.wrapString(KERROR, s, KNRM)

  private val SeparatorLine = wrapString(KWHT, s"--------------------------------------------------------------------------------------\n", KNRM)

  private val PipelineGroupNameColumnLength = 48
  private val PipelineGroupDescriptionColumnLength = 45
  private val PipelineNameColumnLength = 45

  /**
    * Main entry point for the class, takes in the array of arguments from the command line, figueres out
    * how to split them into a) options to dagr, b) the pipeline name to run and c) options to the pipeline,
    * and then parses all of the arguments and constructs the necessary objects.
    */
  def parse(args: Array[String], packageList: List[String]): Option[(DagrCoreMain, Pipeline)] = {
    /* How Usage messages are intended to be printed. The following rules should be applied, in precedence order.
     1. If dagr arguments are missing, always print out the dagr usage.
     2. If no task name is given, just print the dagr usage and the list of available WFs.
     3. If the task is specified and there are missing task arguments, always print out the task usage.
     4. If "-h" is used in dagr's options, print out just the dagr usage.
     5. If "-h" is used in the task's options, print out just the task usage.
     6. If "-H' is used in the task's options, print out both the dagr and task usage.
     */

    // Parse the args for Dagr
    val mainClass = classOf[DagrCoreMain]
    val dagrArgParser = new CommandLineParser(mainClass) {
      override protected def getStandardUsagePreamble: String = {
        s"$KRED$USAGE_PREFIX $KBLDRED$commandLineName$KNRM$KRED [$commandLineName arguments] -- [Task Name] [task arguments]$KNRM\n\n"
      }
      override protected def targetName: String = Configuration.commandLineName
    }

    val print : (String => Unit) = System.err.println
    val errorMessageBuilder: mutable.StringBuilder = new StringBuilder()
    val (dagrArgs, pipelineArgs) = splitArgs(args, packageList, Seq(classOf[DagrCoreMain]), errorMessageBuilder, includeHidden = this.includeHidden)

    if (pipelineArgs.isEmpty) {
      val classes = findPipelineClasses(packageList, omitSubClassesOf = Seq(mainClass), includeHidden = includeHidden).keys.toSet
      print(dagrArgParser.usage())
      print(pipelineListUsage(classes, Configuration.commandLineName))
      print(getUnknownPipeline(Configuration.commandLineName))
      None
    }
    else {
      /////////////////////////////////////////////////////////////////////////
      // Try parsing and building DagrMain and handle the outcomes
      /////////////////////////////////////////////////////////////////////////
      dagrArgParser.parseAndBuild(errorMessageBuilder=errorMessageBuilder, args=dagrArgs) match {
        case ParseResult.Failure =>
          print(dagrArgParser.usage())
          print(wrapError(errorMessageBuilder.toString()))
          None
        case ParseResult.Help =>
          val classes = findPipelineClasses(packageList, omitSubClassesOf = Seq(mainClass), includeHidden = includeHidden).keys.toSet
          print(dagrArgParser.usage())
          print(pipelineListUsage(classes, Configuration.commandLineName))
          None
        case ParseResult.Version =>
          print(dagrArgParser.version)
          None
        case ParseResult.Success =>
          /////////////////////////////////////////////////////////////////////////
          // Get setup, and attempt to ID and load the pipeline class
          /////////////////////////////////////////////////////////////////////////
          val dagr = dagrArgParser.instance.get

          // Load any Dagr scripts and add them to the classpath.
          loadScripts(clp = dagr)

          // Try parsing the task name
          val classToClpAnnotation = findPipelineClasses(packageList, omitSubClassesOf = Seq(mainClass), includeHidden = includeHidden)
          parseTaskName(args = pipelineArgs, classToPropertyMap = classToClpAnnotation) match {
            case Right(error) =>
              print(dagrArgParser.usage())
              print(pipelineListUsage(classToClpAnnotation.keySet, Configuration.commandLineName))
              print(wrapError(error))
              None
            case Left(clazz) =>
              /////////////////////////////////////////////////////////////////////////
              // Parse the arguments for the Pipeline class
              /////////////////////////////////////////////////////////////////////////
              val pipelineParser = new CommandLineParser(clazz)
              pipelineParser.parseAndBuild(errorMessageBuilder, pipelineArgs.drop(1)) match {
                case ParseResult.Failure =>
                  print(dagrArgParser.usage())
                  print(pipelineParser.usage())
                  print(wrapError(errorMessageBuilder.toString()))
                  None
                case ParseResult.Help =>
                  print(dagrArgParser.usage())
                  print(pipelineParser.usage())
                  None
                case ParseResult.Version =>
                  print(pipelineParser.version)
                  None
                case ParseResult.Success =>
                  val pipeline = pipelineParser.instance.get
                  try {
                    dagr.configure(pipeline)
                    Some(dagr, pipeline)
                  }
                  catch {
                    case ex: ValidationException =>
                      print(dagrArgParser.usage())
                      print(pipelineParser.usage())
                      ex.messages.foreach(msg => print(wrapError(msg)))
                      None
                  }
              }
          }
      }
    }
  }

  private def loadScripts(clp: DagrCoreMain): Unit = {
    val scriptLoader = new DagrScriptManager
    scriptLoader.loadScripts(clp.scripts, Files.createTempDirectory(PathUtil.pathTo(System.getProperty("java.io.tmpdir")), "dagrScripts"), quiet = false)
  }

  /** Splits the given args into two Arrays, first splitting based on a "--", and if not found,
    * searching for a program name.  If the user specified a Dagr script, a "--" must always be used.
    * The "--" arg will not be in either returned array.
    */
  private[cmdline] def splitArgs(args: Array[String],
                                 packageList: List[String],
                                 omitSubClassesOf: Iterable[Class[_]] = Nil,
                                 errorMessageBuilder: StringBuilder,
                                 includeHidden: Boolean = false) : (Array[String], Array[String]) = {
    val pipelines = findPipelineClasses(packageList = packageList, omitSubClassesOf = omitSubClassesOf, includeHidden=includeHidden).keys

    // first check for "--"
    args.indexOf("--") match {
      case -1 =>
        // check for Pipeline name
        pipelines.view.map { p => args.indexOf(p.getSimpleName) }.filter(_ != -1).reduceOption(_ min _) match {
          case Some(n) => args.splitAt(n)
          case None => (args, Array[String]())
        }
      case n  =>
        (args.take(n), args.drop(n+1))
    }
  }

  /**
    * Given a package list, a set of args, gets all the relevant command line pipeline classes, and
    * sees if the args(0) matches.  Can print a general usage.
    */
  private def parseTaskName(args: Array[String], classToPropertyMap : Map[PipelineClass, CLPAnnotation]): Either[PipelineClass,String] = {
    val classes: Set[PipelineClass] = classToPropertyMap.keySet

    if (args.length < 1) {
      Right(MISSING_PIPELINE_NAME)
    }
    else {
      val clazzOption: Option[PipelineClass] = classes.find(clazz => 0 == args(0).compareTo(clazz.getSimpleName))
      clazzOption match {
        case Some(clazz) => Left(clazz)
        case None        => Right(unknownPipelineError(classes, args(0)))
      }
    }
  }

  /**
    * Prints the top-level usage, namely the set of command line tasks available.
    *
    * @param classes the classes corresponding to the command line tasks
    * @param commandLineName the name of this command line program (ex. dagr).
    */
  private def pipelineListUsage(classes: Set[PipelineClass], commandLineName: String): String = {
    val builder = new StringBuilder
    builder.append(wrapString(KBLDRED, s"$AVAILABLE_PIPELINES\n", KNRM))
    val taskGroupClassToTaskGroupInstance: mutable.Map[Class[_ <: PipelineGroup], PipelineGroup] = new mutable.HashMap[Class[_ <: PipelineGroup], PipelineGroup]
    val tasksByGroup: java.util.Map[PipelineGroup, ListBuffer[PipelineClass]] = new java.util.TreeMap[PipelineGroup, ListBuffer[PipelineClass]]()
    val tasksToProperty: mutable.Map[PipelineClass, CLPAnnotation] = new mutable.HashMap[PipelineClass, CLPAnnotation]

    classes.foreach {clazz =>
      findClpAnnotation(clazz) match {
        case None => throw new BadAnnotationException(s"The class '${clazz.getSimpleName}' is missing the required CommandLineTaskProperties annotation.")
        case Some(clp) =>
          tasksToProperty.put(clazz, clp)
          var pipelineGroup: Option[PipelineGroup] = taskGroupClassToTaskGroupInstance.get(clp.group)
          if (pipelineGroup.isEmpty) {
            try {
              pipelineGroup = Some(clp.group.newInstance)
            }
            catch {
              case e: InstantiationException => throw new RuntimeException(e)
              case e: IllegalAccessException => throw new RuntimeException(e)
            }
            taskGroupClassToTaskGroupInstance.put(clp.group, pipelineGroup.get)
          }
          var pipelines: ListBuffer[PipelineClass] = tasksByGroup.get(pipelineGroup.get)
          if (null == pipelines) {
            pipelines = ListBuffer[PipelineClass](clazz)
            tasksByGroup.put(pipelineGroup.get, pipelines)
          }
          else {
            pipelines.add(clazz)
          }
      }
    }

    tasksByGroup.entrySet.foreach{entry =>
      val pipelineGroup: PipelineGroup = entry.getKey
      builder.append(SeparatorLine)
      builder.append(wrapString(KRED, String.format(s"%-${PipelineGroupNameColumnLength}s%-${PipelineGroupDescriptionColumnLength}s\n",
        pipelineGroup.name + ":", pipelineGroup.description), KNRM))
      val entries: List[PipelineClass] = entry.getValue.toList
      entries
        .sortWith((lhs, rhs) => lhs.getSimpleName.compareTo(rhs.getSimpleName) < 0)
        .foreach{clazz =>
          val clpAnnotation: CLPAnnotation = tasksToProperty.get(clazz).get
          if (clazz.getSimpleName.length >= PipelineNameColumnLength) {
            builder.append(wrapString(KGRN, String.format(s"    %s    %s\n", clazz.getSimpleName, wrapString(KCYN, formatShortDescription(clpAnnotation.description))), KNRM))
          }
          else {
            builder.append(wrapString(KGRN, String.format(s"    %-${PipelineNameColumnLength}s%s\n",
              clazz.getSimpleName, wrapString(KCYN, formatShortDescription(clpAnnotation.description))), KNRM))
          }
        }
    }

    builder.append(SeparatorLine)
    builder.toString()
  }

  /** The maximum line lengths for pipeline descriptions */
  private val MaximumLineLength = 80

  /** find the first period (".") and keep only everything before it **/
  private def formatShortDescription(description: String): String = {
    val desc = description.stripMargin.dropWhile(_ == '\n')
    desc.indexOf('.') match {
      case -1 =>
        if (desc.length > MaximumLineLength-3) desc.substring(0, MaximumLineLength-3) + "..."
        else desc
      case idx => desc.substring(0, idx+1)
    }
  }

  /** Similarity floor for matching in printUnknown **/
  private val HELP_SIMILARITY_FLOOR: Int = 7
  private val MINIMUM_SUBSTRING_LENGTH: Int = 5

  /** When a command does not match any known command, searches for similar commands, using the same method as GIT **/
  private def unknownPipelineError(classes: Set[PipelineClass], command: String) : String = {
    val builder = new StringBuilder
    val distances: mutable.Map[PipelineClass, Integer] = new mutable.HashMap[PipelineClass, Integer]
    var bestDistance: Int = Integer.MAX_VALUE
    var bestN: Int = 0
    classes.foreach{ clazz =>
      val name: String = clazz.getSimpleName
      if (name == command) {
        throw new CommandLineParserInternalException("Command matches when searching for the unknown: " + command)
      }
      val distance: Int = if (name.startsWith(command) || (MINIMUM_SUBSTRING_LENGTH <= command.length && name.contains(command))) {
        0
      }
      else {
        levenshteinDistance(command, name, 0, 2, 1, 4)
      }
      distances.put(clazz, distance)
      if (distance < bestDistance) {
        bestDistance = distance
        bestN = 1
      }
      else if (distance == bestDistance) {
        bestN += 1
      }
    }
    if (0 == bestDistance && bestN == classes.size) {
      bestDistance = HELP_SIMILARITY_FLOOR + 1
    }
    builder.append(s"${getUnknownCommand(command)}\n")
    if (bestDistance < HELP_SIMILARITY_FLOOR) {
      builder.append(String.format("Did you mean %s?\n", if (bestN < 2) "this" else "one of these"))
      builder.append(classes.filter(bestDistance == distances.get(_).get).mkString("\n        "))
    }

    builder.toString()
  }
}
