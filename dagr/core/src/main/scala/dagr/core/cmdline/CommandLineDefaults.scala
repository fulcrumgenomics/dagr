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
package dagr.core.cmdline

import java.io.File

/**
  * Embodies defaults for global values that affect how the Picard Command Line operates. Defaults are encoded in the class
  * and are also overridable using system properties.
  *
  * @author Nils Homer
  */
object CommandLineDefaults {
  /**
    * Decides if we want to write colors to the terminal.
    */
  val COLOR_STATUS: Boolean = try {
    getBooleanProperty("color_status", `def` = true)
  } catch {
    case _: Throwable => false
  }

  /** Gets a string system property, prefixed with "dagr.core.cmdline." using the default if the property does not exist. */
  private def getStringProperty(name: String, `def`: String): String = {
    System.getProperty("dagr.core.cmdline." + name, `def`)
  }

  /** Gets a boolean system property, prefixed with "dagr.core.cmdline." using the default if the property does not exist. */
  private def getBooleanProperty(name: String, `def`: Boolean): Boolean = {
    val value: String = getStringProperty(name, new java.lang.Boolean(`def`).toString)
    java.lang.Boolean.parseBoolean(value)
  }

  /** Gets an int system property, prefixed with "dagr.core.cmdline." using the default if the property does not exist. */
  private def getIntProperty(name: String, `def`: Int): Int = {
    val value: String = getStringProperty(name, new Integer(`def`).toString)
    value.toInt
  }

  /** Gets a File system property, prefixed with "dagr.core.cmdline." using the default if the property does not exist. */
  private def getFileProperty(name: String, `def`: String): File = {
    val value: String = getStringProperty(name, `def`)
    if (null == value) null else new File(value)
  }
}
