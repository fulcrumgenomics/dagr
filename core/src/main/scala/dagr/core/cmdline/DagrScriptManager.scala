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

import com.fulcrumgenomics.commons.CommonsDef._
import com.fulcrumgenomics.commons.io.Io
import com.fulcrumgenomics.commons.util.LazyLogging
import org.reflections.util.ClasspathHelper

import java.net.{URL, URLClassLoader}
import java.nio.file.Path
import scala.reflect.internal.util.Position
import scala.tools.nsc.io.PlainFile
import scala.reflect.internal.Reporter
import scala.tools.nsc.reporters.{FilteringReporter}
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
  private def addToClasspath(urls: Iterable[URL]): Unit = {
    Thread.currentThread().setContextClassLoader(new URLClassLoader(urls.toArray, Thread.currentThread().getContextClassLoader))
  }

  /**
    * NSC (New Scala Compiler) reporter which logs to Log4J.
    * Heavily based on scala/src/compiler/scala/tools/nsc/reporters/ConsoleReporter.scala
    */
  private class DagrReporter(val settings: Settings) extends FilteringReporter with LazyLogging {
    var errors: Int = 0
    var warnings: Int = 0
    var infos: Int = 0

    override def doReport(pos: Position, msg: String, severity: Severity): Unit = {
      severity match {
        case Reporter.INFO    =>
          infos += 1
          logger.info(msg)
        case Reporter.WARNING =>
          warnings += 1
          logger.warning(msg)
        case Reporter.ERROR   =>
          errors += 1
          logger.error(msg)
      }
    }
  }

  class DagrScriptManagerException(msg: String) extends RuntimeException(msg)
}

private[core] class DagrScriptManager extends LazyLogging {
  import DagrScriptManager._
  /**
    * Compiles and loads the scripts in the files into the current classloader.
    * Heavily based on scala/src/compiler/scala/tools/ant/Scalac.scala
    */
  def loadScripts(scripts: Iterable[Path], tempDir: Path): Unit = {
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

      val reporter = new DagrReporter(settings)
      val compiler: Global = new Global(settings, reporter)
      val run = new compiler.Run

      logger.info("Compiling %s Dagr Script%s".format(scripts.size, plural(scripts.size)))
      logger.debug("Compilation directory: " + settings.outdir.value)
      run.compileFiles(scripts.toList.map(script => new PlainFile(script.toFile)))

      // add `tempDir` to the classpath
      if (!reporter.hasErrors) addToClasspath(urls = Seq(tempDir.toUri.toURL))

      if (reporter.hasErrors) {
        val msg = "Compile of %s failed with %d error%s".format(scripts.mkString(", "), reporter.errors, plural(reporter.errors))
        throw new DagrScriptManagerException(msg)
      }
      else if (reporter.warnings > 0) {
        logger.warning("Compile succeeded with %d warning%s".format(reporter.warnings, plural(reporter.warnings)))
      }
      else {
        logger.info("Compilation complete")
      }
    }
  }
}
