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

// Adapted from GATK's QScriptManager and friends: https://github.com/broadgsa/gatk

package dagr.core.cmdline

import java.net.{URL, URLClassLoader}
import java.nio.file.Path

import dagr.commons.io.Io
import dagr.commons.util.{LazyLogging, LogLevel}
import org.reflections.util.ClasspathHelper

import scala.collection.JavaConversions._
import scala.reflect.internal.util.{FakePos, NoPosition, Position, StringOps}
import scala.tools.nsc.io.PlainFile
import scala.tools.nsc.reporters.AbstractReporter
import scala.tools.nsc.{Global, Settings}

object DagrScriptManager {
  /**
    * Returns the string "s" if x is greater than 1.
    * @param x Value to test.
    * @return "s" if x is greater than one else "".
    */
  private def plural(x: Int) = if (x > 1) "s" else ""

  /**
    * Adds the URL to the system class loader classpath using reflection.
    * HACK: Uses reflection to modify the class path, and assumes loader is a URLClassLoader.
    * @param urls URLs to add to the system class loader classpath.
    */
  private def addToClasspath(urls: Traversable[URL]): Unit = {
    Thread.currentThread().setContextClassLoader(new URLClassLoader(urls.toArray, Thread.currentThread().getContextClassLoader))
  }

  /**
    * NSC (New Scala Compiler) reporter which logs to Log4J.
    * Heavily based on scala/src/compiler/scala/tools/nsc/reporters/ConsoleReporter.scala
    */
  private class DagrReporter(val settings: Settings, val quiet: Boolean = false) extends AbstractReporter with LazyLogging {
    def displayPrompt(): Unit = {
      throw new UnsupportedOperationException("Unable to prompt the user.  Prompting should be off.")
    }

    /**
      * Displays the message at position with severity.
      * @param posIn Position of the event in the file that generated the message.
      * @param msg Message to display.
      * @param severity Severity of the event.
      */
    def display(posIn: Position, msg: String, severity: Severity): Unit = {
      severity.count += 1
      val level = severity match {
        case INFO => LogLevel.Info
        case WARNING => LogLevel.Warning
        case ERROR => LogLevel.Error
      }

      val p2 = Option(posIn) match {
        case None => NoPosition
        case Some(p) if p.isDefined => p.finalPosition //posIn.inUltimateSource(posIn.source)
        case Some(p) => p
      }

      p2 match {
        case FakePos(fmsg) =>
          printMessage(level, s"$fmsg $msg")
        case NoPosition =>
          printMessage(level, msg)
        case pos: Position =>
          val file = pos.source.file
          printMessage(level, file.name + ":" + pos.line + ": " + msg)
          printSourceLine(level, pos)
      }
    }

    /**
      * Prints the source code line of an event followed by a pointer within the line to the error.
      * @param level Severity level.
      * @param pos Position in the file of the event.
      */
    private def printSourceLine(level: LogLevel, pos: Position): Unit = {
      printMessage(level, pos.lineContent.stripLineEnd)
      printColumnMarker(level, pos)
    }

    /**
      * Prints the column marker of the given position.
      * @param level Severity level.
      * @param pos Position in the file of the event.
      */
    private def printColumnMarker(level: LogLevel, pos: Position): Unit = {
      if (pos.isDefined) {
        printMessage(level, " " * (pos.column - 1) + "^")
      }
    }

    /**
      * Prints a summary count of warnings and errors.
      */
    def printSummary(): Unit = {
      if (WARNING.count > 0)
        printMessage(LogLevel.Warning, StringOps.countElementsAsString(WARNING.count, "warning") + " found")
      if (ERROR.count > 0)
        printMessage(LogLevel.Error, StringOps.countElementsAsString(ERROR.count, "error") + " found")
    }

    /**
      * Prints the message at the severity level.
      * @param level Severity level.
      * @param message Message content.
      */
    private def printMessage(level: LogLevel, message: String): Unit = {
      if (!quiet) {
        level match {
          case LogLevel.Debug   => logger.debug(message)
          case LogLevel.Info    => logger.info(message)
          case LogLevel.Warning => logger.warning(message)
          case LogLevel.Error   => logger.error(message)
          case LogLevel.Fatal   => logger.fatal(message)
          case _ => throw new RuntimeException(s"Could not determine log level: $level")
        }
      }
    }
  }
}

private[core] class DagrScriptManager extends LazyLogging {
  import DagrScriptManager._
  /**
    * Compiles and loads the scripts in the files into the current classloader.
    * Heavily based on scala/src/compiler/scala/tools/ant/Scalac.scala
    */
  def loadScripts(scripts: Traversable[Path], tempDir: Path, quiet: Boolean = true): Unit = {
    // Make sure the scripts actually exist and we can write to the tempDir
    Io.assertReadable(scripts)
    Io.assertWritableDirectory(tempDir)

    // Do nothing if we have nothing to load
    if (scripts.nonEmpty) {
      val settings = new Settings((error: String) => logger.error(error))
      settings.deprecation.value = true
      settings.outdir.value = tempDir.toString

      // Set the classpath to the current class path.
      ClasspathHelper.forManifest.foreach(url => {
        settings.bootclasspath.append(url.getPath)
        settings.classpath.append(url.getPath)
      })

      val reporter = new DagrReporter(settings, quiet)
      val compiler: Global = new Global(settings, reporter)
      val run = new compiler.Run

      if (!quiet) {
        logger.info("Compiling %s Dagr Script%s".format(scripts.size, plural(scripts.size)))
        logger.debug("Compilation directory: " + settings.outdir.value)
      }
      run.compileFiles(scripts.toList.map(script => new PlainFile(script.toFile)))

      // add `tempDir` to the classpath
      if (!reporter.hasErrors) addToClasspath(urls = Seq(tempDir.toUri.toURL))

      reporter.printSummary()
      if (reporter.hasErrors) {
        val msg = "Compile of %s failed with %d error%s".format(scripts.mkString(", "), reporter.ERROR.count, plural(reporter.ERROR.count))
        throw new RuntimeException(msg)
      }
      else if (reporter.WARNING.count > 0) {
        if (!quiet) logger.warning("Compile succeeded with %d warning%s".format(reporter.WARNING.count, plural(reporter.WARNING.count)))
      }
      else {
        if (!quiet) logger.info("Compilation complete")
      }
    }
  }
}
