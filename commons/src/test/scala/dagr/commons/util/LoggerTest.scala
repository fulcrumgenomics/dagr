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

import java.io.{PrintStream, ByteArrayOutputStream}

import org.scalatest.{SequentialNestedSuiteExecution, BeforeAndAfterEach}

class LoggerTest extends UnitSpec with LazyLogging with BeforeAndAfterEach with SequentialNestedSuiteExecution {

  private val level: LogLevel = Logger.level

  override def afterEach(): Unit = {
    Logger.level = level
  }

  "Logger.sanitizeSimpleClassName" should "display scala classes in a readable form" in {
    Logger.sanitizeSimpleClassName(classOf[LoggerTest].getSimpleName) shouldBe "LoggerTest"
  }

  "Logger" should "log an exception" in {
    val logger = new Logger(classOf[LoggerTest])
    val stream = new ByteArrayOutputStream()
    logger.out = Some(new PrintStream(stream))
    val ex = new IllegalArgumentException("Arg!")
    logger.exception(ex)
    stream.toString should include ("Arg!")
  }

  it should "log a message or throwable when the logger is at the same level" in {
    // synchronized since we modify the log level, and we don't want to affect other tests
    this.synchronized {
      val logLevel = Logger.level
      LogLevel.values().foreach { level =>
        Logger.level = level

        val logger = new Logger(classOf[LoggerTest])
        val stream = new ByteArrayOutputStream()
        val pw = new PrintStream(stream)
        logger.out = Some(pw)

        logger.debug("ThisIsDebug")
        logger.debug(new IllegalArgumentException("DebugException"), "DebugException")

        logger.info("ThisIsInfo")
        logger.info(new IllegalArgumentException("InfoException"), "InfoException")

        logger.warning("ThisIsWarning")
        logger.warning(new IllegalArgumentException("WarningException"), "WarningException")

        logger.error("ThisIsError")
        logger.error(new IllegalArgumentException("ErrorException"), "ErrorException")

        logger.fatal("ThisIsFatal")
        logger.fatal(new IllegalArgumentException("FatalException"), "FatalException")

        val output = stream.toString
        if (level.ordinal() <= LogLevel.Debug.ordinal()) output should include("ThisIsDebug") else output shouldNot include("ThisIsDebug")
        if (level.ordinal() <= LogLevel.Debug.ordinal()) output should include("DebugException") else output shouldNot include("DebugException")

        if (level.ordinal() <= LogLevel.Info.ordinal()) output should include("ThisIsInfo") else output shouldNot include("ThisIsInfo")
        if (level.ordinal() <= LogLevel.Info.ordinal()) output should include("InfoException") else output shouldNot include("InfoException")

        if (level.ordinal() <= LogLevel.Warning.ordinal()) output should include("ThisIsWarning") else output shouldNot include("ThisIsWarning")
        if (level.ordinal() <= LogLevel.Warning.ordinal()) output should include("WarningException") else output shouldNot include("WarningException")

        if (level.ordinal() <= LogLevel.Error.ordinal()) output should include("ThisIsError") else output shouldNot include("ThisIsError")
        if (level.ordinal() <= LogLevel.Error.ordinal()) output should include("ErrorException") else output shouldNot include("ErrorException")

        if (level.ordinal() <= LogLevel.Fatal.ordinal()) output should include("ThisIsFatal") else output shouldNot include("ThisIsFatal")
        if (level.ordinal() <= LogLevel.Fatal.ordinal()) output should include("FatalException") else output shouldNot include("FatalException")
      }
      Logger.level = logLevel // turn logging back on
    }
  }

  it should "not log an debug message when the logger is at the info level" in {
    Logger.level shouldBe LogLevel.Info // verify
    val logger = new Logger(classOf[LoggerTest])
    val stream = new ByteArrayOutputStream()
    val pw = new PrintStream(stream)
    logger.out = Some(pw)
    logger.debug("Arg!")
    stream.toString shouldBe 'empty
  }

  it should "have a logger when mixing in LazyLogging" in {
    Logger.level shouldBe LogLevel.Info // verify
    val stream = new ByteArrayOutputStream()
    val pw = new PrintStream(stream)
    logger.out = Some(pw)
    logger.info("Arg!")
    stream.toString should include ("Arg!")
    logger.out = None
  }
}
