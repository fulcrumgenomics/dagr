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
package dagr.commons.util

import java.io.{PrintStream, PrintWriter, StringWriter}
import java.text.SimpleDateFormat
import java.util.Date

/** Companion object for the Logger class that holds the system-wide log level, and a PrintWriter to write to. */
object Logger {
  var level = LogLevel.Info
  protected var out: PrintStream = System.out

  /** Removes various characters from the simple class name, for scala class names. */
  private[util] def sanitizeSimpleClassName(className: String): String = {
    className.replaceFirst("[$].*$", "")
  }
}

/**
 * Very simple logging class that supports logging at multiple levels.
 */
class Logger(clazz : Class[_]) {
  private val name = Logger.sanitizeSimpleClassName(clazz.getSimpleName)
  private val fmt = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  var out: Option[PrintStream] = None

  /** Checks to see if a message should be emitted given the current log level, and then emits atomically. */
  protected def emit(l: LogLevel, parts: TraversableOnce[Any]) : Unit = {
    if (l.compareTo(Logger.level) >= 0) {
      val builder = new StringBuilder(256)
      builder.append("[").append(fmt.format(new Date())).append(" | ").append(name).append(" | ").append(l.toString).append("] ")
      parts.foreach(part => builder.append(part))
      this.out match {
        case Some(o) => o.println(builder.toString())
        case _ => Logger.out.println(builder.toString())
      }
    }
  }

  /** Logs an exception at error. */
  def exception(t: Throwable, parts: Any*) : Unit = exception(t, LogLevel.Error, parts)

  /** Logs an exception if at the appropriate level. */
  private def exception(t: Throwable, l: LogLevel, parts: Any*) : Unit = {
    if (l.compareTo(Logger.level) >= 0) {
      val stringWriter = new StringWriter
      t.printStackTrace(new PrintWriter(stringWriter))
      if (LogLevel.Fatal.ordinal() <= l.ordinal()) emit(l=l, parts :+ stringWriter.toString)
      else error(parts :+ stringWriter.toString)
    }
  }

  def debug  (parts: Any*) : Unit = emit(LogLevel.Debug,   parts)
  def info   (parts: Any*) : Unit = emit(LogLevel.Info,    parts)
  def warning(parts: Any*) : Unit = emit(LogLevel.Warning, parts)
  def error  (parts: Any*) : Unit = emit(LogLevel.Error,   parts)
  def fatal  (parts: Any*) : Unit = emit(LogLevel.Fatal,   parts)

  // for java
  def debug  (parts: String) : Unit = emit(LogLevel.Debug,   parts)
  def info   (parts: String) : Unit = emit(LogLevel.Info,    parts)
  def warning(parts: String) : Unit = emit(LogLevel.Warning, parts)
  def error  (parts: String) : Unit = emit(LogLevel.Error,   parts)
  def fatal  (parts: String) : Unit = emit(LogLevel.Fatal,   parts)

  def debug  (t: Throwable, parts: Any*) : Unit = exception(t, LogLevel.Debug,   parts)
  def info   (t: Throwable, parts: Any*) : Unit = exception(t, LogLevel.Info,    parts)
  def warning(t: Throwable, parts: Any*) : Unit = exception(t, LogLevel.Warning, parts)
  def error  (t: Throwable, parts: Any*) : Unit = exception(t, LogLevel.Error,   parts)
  def fatal  (t: Throwable, parts: Any*) : Unit = exception(t, LogLevel.Fatal,   parts)

  // for java
  def debug  (t: Throwable, parts: String) : Unit = exception(t, LogLevel.Debug,   parts)
  def info   (t: Throwable, parts: String) : Unit = exception(t, LogLevel.Info,    parts)
  def warning(t: Throwable, parts: String) : Unit = exception(t, LogLevel.Warning, parts)
  def error  (t: Throwable, parts: String) : Unit = exception(t, LogLevel.Error,   parts)
  def fatal  (t: Throwable, parts: String) : Unit = exception(t, LogLevel.Fatal,   parts)


}

/**
 * Trait that can be mixed into classes to provide a Logger that is constructed at first access.
 */
trait LazyLogging {
  protected lazy val logger: Logger = new Logger(getClass)
}
