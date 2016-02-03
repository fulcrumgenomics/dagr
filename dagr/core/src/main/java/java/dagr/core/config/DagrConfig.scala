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
package dagr.core.config

import java.nio.file.Path

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}

/** This contains all the default configuration strings that Dagr may care about. */
object DagrConfigPaths {
  val CommandLineName  = "dagr.command-line-name"
  val ScriptDirectory  = "dagr.script-directory"
  val LogDirectory     = "dagr.log-directory"
  val SystemCores      = "dagr.system-cores"
  val SystemMemory     = "dagr.system-memory"
}

/** Contains the Dagr configuration.  It may be useful to supply your own configuration file for custom options
  * to your given task.  Access those values here.  The configuration file may be: a Java properties file,
  * a JSON file, or a "human-friendly" JSON file.  For more information, see: https://github.com/typesafehub/config
  */
private[core] object DagrConfig {
  import DagrConfigPaths._
  private var configInstance = ConfigFactory.load()

  def config: Config = this.configInstance

  /** The name of this unified command line program **/
  def getCommandLineName: String = {
    if (getConfig.hasPath(CommandLineName)) getConfig.getString(CommandLineName)
    else "Dagr"
  }

  /** Gets the Dagr configuration data */
  def getConfig: Config = config

  /** Load the configuration options from the given path, replacing the current config */
  def initialize(path: Option[Path]): Unit = path match {
    case None    =>
      this.configInstance = ConfigFactory.load()
    case Some(p) =>
      // setAllowMissing(false) refers to allowing the file(!) to be missing, not values within the file
      this.configInstance = ConfigFactory.load(ConfigFactory.parseFile(p.toFile, ConfigParseOptions.defaults().setAllowMissing(false)))
  }
}
