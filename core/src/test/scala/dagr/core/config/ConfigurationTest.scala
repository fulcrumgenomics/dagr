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

import java.nio.file.{Files, Path}
import java.time.Duration

import com.typesafe.config.ConfigFactory
import dagr.core.execsystem.{Cores, Memory}
import dagr.core.util.{PathUtil, UnitSpec}

/**
  * Tests for the Configuration trait.
  */
class ConfigurationTest extends UnitSpec {
  val _config = ConfigFactory.parseString(
    """
      |dagr.path = ${PATH}
      |a-string = hello
      |a-boolean = true
      |a-short  = 123
      |an-int   = 12345
      |a-long   = 1234567890
      |a-float  = 12345.67
      |a-double = 12345.6789
      |a-bigint = 999999999999999999999999999999999999999999999999999999999999999
      |a-bigdec = 999999999999999999999999999999999999999999999999999999999999999.1
      |a-path   = /foo/bar/splat.txt
      |some-cores  = 2.5
      |some-memory = 2G
      |some-time   = 60s
      |some-executable = /does/not/exist
      |    """.stripMargin)

  val conf = new Configuration { override val config = _config.resolve() }

  "Configuration" should "lookup basic types with keys that exist" in {
    conf.configure[String]("a-string") shouldBe "hello"
    conf.configure[Boolean]("a-boolean") shouldBe true
    conf.configure[Short]("a-short") shouldBe 123
    conf.configure[Int]("an-int") shouldBe 12345
    conf.configure[Long]("a-long") shouldBe 1234567890
    conf.configure[Float]("a-float") shouldBe 12345.67f
    conf.configure[Double]("a-double") shouldBe 12345.6789
    conf.configure[BigInt]("a-bigint") shouldBe BigInt("999999999999999999999999999999999999999999999999999999999999999")
    conf.configure[BigDecimal]("a-bigdec") shouldBe BigDecimal("999999999999999999999999999999999999999999999999999999999999999.1")
    conf.configure[Path]("a-path") shouldBe PathUtil.pathTo("/foo/bar/splat.txt")
    conf.configure[Cores]("some-cores") shouldBe Cores(2.5)
    conf.configure[Memory]("some-memory") shouldBe Memory("2g")
    conf.configure[Duration]("some-time") shouldBe Duration.ofSeconds(60)
  }

  it should "support optional configuration" in {
    conf.optionallyConfigure[String]("a-string") shouldBe Some("hello")
    // The following should all behave the same, but check a few just in case
    conf.optionallyConfigure[Long]("non-existent-key") shouldBe None
    conf.optionallyConfigure[Path]("non-existent-key") shouldBe None
    conf.optionallyConfigure[Cores]("non-existent-key") shouldBe None
    conf.optionallyConfigure[Duration]("non-existent-key") shouldBe None
  }

  it should "support default values" in {
    // All these exist, defaults should be ignored
    conf.configure[String]("a-string", "wont-get-this") shouldBe "hello"
    conf.configure[Boolean]("a-boolean", false) shouldBe true
    conf.configure[Long]("a-long", 999) shouldBe 1234567890
    conf.configure[Float]("a-float", 999f) shouldBe 12345.67f
    conf.configure[Path]("a-path", PathUtil.pathTo("/path/to/nowhere")) shouldBe PathUtil.pathTo("/foo/bar/splat.txt")
    conf.configure[Cores]("some-cores", Cores(50f)) shouldBe Cores(2.5)
    conf.configure[Memory]("some-memory", Memory("128G")) shouldBe Memory("2g")

    // And these should all return their defaults
    conf.configure[String]("not-a-string", "wont-get-this") shouldBe "wont-get-this"
    conf.configure[Boolean]("not.a-boolean", false) shouldBe false
    conf.configure[Long]("foo.bar.splat.a-long", 999) shouldBe 999
    conf.configure[Float]("no-float", 999f) shouldBe 999f
    conf.configure[Path]("xxx", PathUtil.pathTo("/path/to/nowhere")) shouldBe PathUtil.pathTo("/path/to/nowhere")
    conf.configure[Cores]("no-cores.here", Cores(50f)) shouldBe Cores(50.0)
    conf.configure[Memory]("i.forgot", Memory("128G")) shouldBe Memory("128g")
  }

  it should "find executables" in {
    conf.configureExecutable("some-executable", "n/a") shouldBe PathUtil.pathTo("/does/not/exist")

    val java = conf.configureExecutable("java.exe", "java")
    java.getFileName.toString shouldBe "java"
    Files.isExecutable(java) shouldBe true
  }
}
