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

import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
import com.typesafe.config.ConfigException.Generic
import com.fulcrumgenomics.commons.io.PathUtil._
import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.exec.{Cores, Memory}

import scala.collection.SortedSet
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import collection.JavaConversions._

/**
  * Companion object to the Configuration trait that keeps track of all configuration keys
  * that have been requested so that they can be reported later if desired.
  */
object Configuration extends ConfigurationLike {
  private[config] val RequestedKeys = collection.mutable.TreeSet[String]()

  // Keys for configuration values used in dagr core
  object Keys {
    val CommandLineName = "dagr.command-line-name"
    val ColorStatus     = "dagr.color-status"
    val SystemPath      = "dagr.path"
    val PackageList     = "dagr.package-list"
    val ScriptDirectory = "dagr.script-directory"
    val LogDirectory    = "dagr.log-directory"
    val SystemCores     = "dagr.system-cores"
    val SystemMemory    = "dagr.system-memory"
    val PrintArgs       = "dagr.print-args"
  }

  // The global configuration instance
  protected var _config: Config = ConfigFactory.load()

  protected def config = _config

  /**
    * Initialize the configuration by loading configuration from the supplied path, and combining it with
    * configuration information from the system properties (higher priority), application.conf and
    * reference.conf files (lower priority).
    */
  def initialize(path: Option[Path]): Unit = path match {
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

  /** Returns a sorted set of all keys that have been requested up to this point in time. */
  def requestedKeys: SortedSet[String] = {
    var keys = collection.immutable.TreeSet[String]()
    keys ++= RequestedKeys
    keys
  }

  /** Allows initialization with a custom configuration. */
  protected def initialize(customConfig: Config): Unit = this._config = customConfig
}

/**
  * Trait that provides useful methods for resolving all kinds of things in configuration,
  * that is then mixed into the Configuration object above, and used to create a concrete
  * Configuration trait below.
  */
private[config] trait ConfigurationLike extends LazyLogging {
  protected def config : Config

  /** Converts the configuration path to the given type. If the requested type is not supported, an
    * exception is thrown.  Override this method to support custom types.
    */
  protected def asType[T : TypeTag](path: String) : T = {
    typeOf[T] match {
      case t if t =:= typeOf[String]       => config.getString(path).asInstanceOf[T]
      case t if t =:= typeOf[Boolean]      => config.getBoolean(path).asInstanceOf[T]
      case t if t =:= typeOf[Short]        => config.getInt(path).toShort.asInstanceOf[T]
      case t if t =:= typeOf[Int]          => config.getInt(path).asInstanceOf[T]
      case t if t =:= typeOf[Long]         => config.getLong(path).asInstanceOf[T]
      case t if t =:= typeOf[Float]        => config.getDouble(path).toFloat.asInstanceOf[T]
      case t if t =:= typeOf[Double]       => config.getDouble(path).asInstanceOf[T]
      case t if t =:= typeOf[BigInt]       => BigInt(config.getString(path)).asInstanceOf[T]
      case t if t =:= typeOf[BigDecimal]   => BigDecimal(config.getString(path)).asInstanceOf[T]
      case t if t =:= typeOf[Path]         => pathTo(config.getString(path)).asInstanceOf[T]
      case t if t =:= typeOf[Duration]     => config.getDuration(path).asInstanceOf[T]
      // TODO: replace this with better handling of List/Seq/Array
      case t if t =:= typeOf[List[String]] => config.getStringList(path).toList.asInstanceOf[T]
      case t if t =:= typeOf[Cores] => Cores(config.getDouble(path)).asInstanceOf[T]
      case t if t =:= typeOf[Memory] => Memory(config.getString(path)).asInstanceOf[T]
      case _ => throw new IllegalArgumentException("Don't know how to configure a " + typeOf[T])
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
    * Looks up a single value of a specific type in configuration. If the configuration key
    * does not exist, an exception is thrown. If the requested type is not supported, an
    * exception is thrown.
    */
  def configure[T : TypeTag](path: String) : T = {
    Configuration.RequestedKeys += path
    try {
      asType[T](path)
    }
    catch {
      case ex : Exception =>
        throw new IllegalArgumentException(s"Exception retrieving configuration key '$path': ${ex.getMessage}", ex)
    }
  }

  /**
    * Looks up a value in the configuration, and if present returns it, otherwise returns the default
    * value provided.
    */
  def configure[T : TypeTag](path: String, defaultValue: T) : T = {
    try {
      Configuration.RequestedKeys += path
      if (config.hasPath(path)) configure[T](path)
      else defaultValue
    }
    catch {
      case ex : Exception =>
        logger.exception(ex)
        throw ex
    }
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
    * @param subDir optionally a sub-directory within the bin-directory containing the executable.
    * @return An absolute path to the executable to use
    */
  def configureExecutableFromBinDirectory(binPath: String, executable: String, subDir: Option[Path] = None) : Path = {
    Configuration.RequestedKeys += binPath

    optionallyConfigure[Path](binPath) match {
      case Some(exec) =>
        subDir match {
          case Some(dir) => pathTo(exec.toString, dir.toString, executable)
          case None      => pathTo(exec.toString, executable)
        }
      case None => findInPath(executable) match {
        case Some(exec) => exec
        case None => throw new Generic(s"Could not configurable executable. Config path '$binPath' is not defined and executable '$executable' is not in PATH.")
      }
    }
  }

  def commandLineName(name: String = "dagr"): String = {
    configure(Configuration.Keys.CommandLineName, name)
  }

  /**
    * Grabs the config key "PATH" which, if not defined in config will default to the environment variable
    * PATH, splits it on the path separator and returns it as a Seq[String]
    */
  protected def systemPath : Seq[Path] = config.getString(Configuration.Keys.SystemPath).split(File.pathSeparatorChar).view.map(pathTo(_))

  /** Removes various characters from the simple class name, for scala class names. */
  private def sanitizeSimpleClassName(className: String): String = {
    className.replaceFirst("[$].*$", "")
  }

  /** Searches the system path for the executable and return the full path. */
  private def findInPath(executable: String) : Option[Path] = {
    systemPath.map(p => p.resolve(executable)).find(ex => Files.exists(ex))
  }
}

/** The trait to be mixed in to access configuration */
trait Configuration extends ConfigurationLike {
  /** Grabs a reference to the global configuration at creation time. */
  protected def config : Config = Configuration.config
}
