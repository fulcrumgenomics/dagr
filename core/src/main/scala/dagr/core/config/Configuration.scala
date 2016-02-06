/*
 * The MIT License
 *
 * Copyright (c) 2016 Fulcrum Genomics LLC
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

import java.io.File
import java.nio.file.{Files, Path}
import java.time.Duration

import com.typesafe.config.ConfigException.Generic
import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import dagr.core.execsystem.{Cores, Memory}
import dagr.core.util.{LazyLogging, PathUtil}

import scala.collection.JavaConversions._
import scala.collection.SortedSet
import scala.reflect.runtime.universe.{TypeTag, typeOf}


// Keys for configuration values used in dagr core
object ConfigurationKeys {
  val CommandLineName = "dagr.command-line-name"
  val PackageList     = "dagr.package-list"
  val ScriptDirectory = "dagr.script-directory"
  val LogDirectory    = "dagr.log-directory"
  val SystemCores     = "dagr.system-cores"
  val SystemMemory    = "dagr.system-memory"
  val ColorStatus     = "dagr.color-status"
}

/**
  * Companion object to the Configuration trait that keeps track of all configuration keys
  * that have been requested so that they can be reported later if desired.
  */
private[core] object Configuration extends ConfigurationLike {
  // Developer Note: [[Configuration]] is not private so that [[Configuration.Keys]] is public

  // A sorted set tracking all the configuration keys that are requested
  private[config] val RequestedKeys = collection.mutable.TreeSet[String]()

  // The global configuration instance for dagr
  private[config] var _config: Config = ConfigFactory.load()

  /** Implement the abstract method from BaseConfiguration to return the actual config instance. */
  override private[config] def config = _config

  /**
    * Initialize the configuration by loading configuration from the supplied path, and combining it with
    * configuration information from the system properties (higher priority), application.conf and
    * reference.conf files (lower priority).
    */
  private[core] def initialize(path: Option[Path]): Unit = path match {
    case None    =>
      this._config = ConfigFactory.load()
    case Some(p) =>
      // setAllowMissing(false) refers to allowing the file(!) to be missing, not values within the file
      val options = ConfigParseOptions.defaults().setAllowMissing(false)
      val localConfig = ConfigFactory.parseFile(p.toFile, options)

      // This mimics the behaviour of ConfigFactory.load() but with the localConfig sandwiched in
      this._config = ConfigFactory.defaultOverrides()
        .withFallback(localConfig)
        .withFallback(ConfigFactory.defaultApplication())
        .withFallback(ConfigFactory.defaultReference())
        .resolve()
  }

  /** Allows initialization with a custom configuration. */
  private[core] def initialize(customConfig: Config): Unit = this._config = customConfig

  /** Returns a sorted set of all keys that have been requested up to this point in time. */
  private[core] def requestedKeys: SortedSet[String] = {
    var keys = collection.immutable.TreeSet[String]()
    keys ++= RequestedKeys
    keys
  }

  /** The name of this unified command line program **/
  private[core] def commandLineName: String = configure(ConfigurationKeys.CommandLineName, "Dagr")
}

/**
  * Trait that provides useful methods for resolving all kinds of things in configuration,
  * that is then mixed into the Configuration object above, and used to create a concrete
  * Configuration trait below.
  */
private[config] trait ConfigurationLike extends LazyLogging {
  private[config] def config : Config

  /**
    * Looks up a single value of a specific type in configuration. If the configuration key
    * does not exist, an exception is thrown. If the requested type is not supported, an
    * exception is thrown.
    */
  def configure[T : TypeTag](path: String) : T = {
    Configuration.RequestedKeys += path

    try {
      typeOf[T] match {
        case t if t =:= typeOf[String] => config.getString(path).asInstanceOf[T]
        case t if t =:= typeOf[Boolean] => config.getBoolean(path).asInstanceOf[T]
        case t if t =:= typeOf[Short] => config.getInt(path).toShort.asInstanceOf[T]
        case t if t =:= typeOf[Int] => config.getInt(path).asInstanceOf[T]
        case t if t =:= typeOf[Long] => config.getLong(path).asInstanceOf[T]
        case t if t =:= typeOf[Float] => config.getDouble(path).toFloat.asInstanceOf[T]
        case t if t =:= typeOf[Double] => config.getDouble(path).asInstanceOf[T]
        case t if t =:= typeOf[BigInt] => BigInt(config.getString(path)).asInstanceOf[T]
        case t if t =:= typeOf[BigDecimal] => BigDecimal(config.getString(path)).asInstanceOf[T]
        case t if t =:= typeOf[Path] => PathUtil.pathTo(config.getString(path)).asInstanceOf[T]
        case t if t =:= typeOf[Cores] => Cores(config.getDouble(path)).asInstanceOf[T]
        case t if t =:= typeOf[Memory] => Memory(config.getString(path)).asInstanceOf[T]
        case t if t =:= typeOf[Duration] => config.getDuration(path).asInstanceOf[T]
        // TODO: replace this with better handling of List/Seq/Array
        case t if t =:= typeOf[List[String]] => config.getStringList(path).toList.asInstanceOf[T]
        case _ => throw new IllegalArgumentException("Don't know how to configure a " + typeOf[T])
      }
    }
    catch {
      case ex : Exception =>
        logger.error(s"#############################################################################")
        logger.error(s"Exception retrieving configuration key '$path': ${ex.getMessage}")
        logger.error(s"#############################################################################")
        throw ex
    }
  }

  /**
    * Optionally accesses a configuration value. If the value is not present in the configuration
    * a None will be returned, else a Some(T) of the appropriate type.
    */
  def optionallyConfigure[T : TypeTag](path: String) : Option[T] = {
    Configuration.RequestedKeys += path
    if (config.hasPath(path)) Some(configure[T](path))
    else None
  }

  /**
    * Looks up a value in the configuration, and if present returns it, otherwise returns the default
    * value provided.
    */
  def configure[T : TypeTag](path: String, defaultValue: T) : T = {
    Configuration.RequestedKeys += path
    if (config.hasPath(path)) configure[T](path)
    else defaultValue
  }

  /**
    * Attempts to determine the path to an executable, first by looking it up in config,
    * and if that fails, by attempting to locate it on the system path. If both fail then
    * an exception is raised.
    *
    * @param path the configuration path to look up
    * @param executable the default name of the executable
    * @return An absolute path to the executable to use
    */
  def configureExecutable(path: String, executable: String) : Path = {
    Configuration.RequestedKeys += path

    optionallyConfigure[Path](path) match {
      case Some(exec) => exec
      case None => findInPath(executable) match {
        case Some(exec) => exec
        case None => throw new Generic(s"Could not configurable executable. Config path '$path' is not defined and executable '$executable' is not in PATH.")
      }
    }
  }

  /**
    * Attempts to determine the path to an executable.
    *
    * The config path is assumed to be the directory containing the executable.  If the config lookup fails, then we
    * attempt to locate it on the system path. If both fail then an exception is raised.  No validation is performed
    * that the executable actually exists at the returned path.
    *
    * @param binPath the configuration path to look up, representing the directory containing the executable
    * @param executable the default name of the executable
    * @return An absolute path to the executable to use
    */
  def configureExecutableFromBinDirectory(binPath: String, executable: String) : Path = {
    Configuration.RequestedKeys += binPath

    optionallyConfigure[Path](binPath) match {
      case Some(exec) =>
        PathUtil.pathTo(exec.toString, executable)
      case None => findInPath(executable) match {
        case Some(exec) => exec
        case None => throw new Generic(s"Could not configurable executable. Config path '$binPath' is not defined and executable '$executable' is not in PATH.")
      }
    }
  }

  /** Searches the system path for the executable and return the full path. */
  private def findInPath(executable: String) : Option[Path] = {
    systemPath.map(p => p.resolve(executable)).find(ex => Files.exists(ex))
  }

  /**
    * Grabs the config key "PATH" which, if not defined in config will default to the environment variable
    * PATH, splits it on the path separator and returns it as a Seq[String]
    */
  private def systemPath : Seq[Path] = config.getString("dagr.path").split(File.pathSeparatorChar).view.map(PathUtil pathTo _)
}

trait Configuration extends ConfigurationLike {
  /** Grabs a reference to the global configuration at creation time. */
  override private[config] val config: Config = Configuration.config
}
