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

import java.io.PrintWriter
import java.nio.file.{Files, Path, Paths}
import java.time.Duration

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import com.fulcrumgenomics.commons.io.{Io, PathUtil}
import com.fulcrumgenomics.commons.util.CaptureSystemStreams
import dagr.core.UnitSpec
import dagr.core.execsystem.{Cores, Memory}

/**
  * Tests for the Configuration trait.
  */
class ConfigurationTest extends UnitSpec with CaptureSystemStreams {
  private val _config = ConfigFactory.parseString(
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
      |some-time   = 60s
      |some-string-list = ["a", list, "of", strings]
      |a-path   = /foo/bar/splat.txt
      |some-cores  = 2.5
      |some-memory = 2G
      |some-executable = /does/not/exist
      |a-string-set = ["A", "B", "C"]
      |    """.stripMargin)

  val conf = new Configuration { override val config : Config = _config.resolve() }

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
    conf.configure[Duration]("some-time") shouldBe Duration.ofSeconds(60)
    conf.configure[List[String]]("some-string-list") shouldBe List[String]("a", "list", "of", "strings")
    conf.configure[Path]("a-path") shouldBe PathUtil.pathTo("/foo/bar/splat.txt")
    conf.configure[Cores]("some-cores") shouldBe Cores(2.5)
    conf.configure[Memory]("some-memory") shouldBe Memory("2g")
  }

  it should "support optional configuration" in {
    conf.optionallyConfigure[String]("a-string") shouldBe Some("hello")
    // The following should all behave the same, but check a few just in case
    conf.optionallyConfigure[Long]("non-existent-key") shouldBe None
    conf.optionallyConfigure[Path]("non-existent-key") shouldBe None
    conf.optionallyConfigure[Duration]("non-existent-key") shouldBe None
    conf.optionallyConfigure[Cores]("non-existent-key") shouldBe None
  }

  it should "support default values" in {
    // All these exist, defaults should be ignored
    conf.configure[String]("a-string", "wont-get-this") shouldBe "hello"
    conf.configure[Boolean]("a-boolean", false) shouldBe true
    conf.configure[Long]("a-long", 999) shouldBe 1234567890
    conf.configure[Float]("a-float", 999f) shouldBe 12345.67f
    conf.configure[Path]("a-path", PathUtil.pathTo("/path/to/nowhere")) shouldBe PathUtil.pathTo("/foo/bar/splat.txt")
    conf.configure[Path]("a-path", PathUtil.pathTo("/path/to/nowhere")) shouldBe PathUtil.pathTo("/foo/bar/splat.txt")
    conf.configure[Cores]("some-cores", Cores(50f)) shouldBe Cores(2.5)
    conf.configure[Memory]("some-memory", Memory("128G")) shouldBe Memory("2g")

    // And these should all return their defaults
    conf.configure[String]("not-a-string", "wont-get-this") shouldBe "wont-get-this"
    conf.configure[Boolean]("not.a-boolean", false) shouldBe false
    conf.configure[Long]("foo.bar.splat.a-long", 999) shouldBe 999
    conf.configure[Float]("no-float", 999f) shouldBe 999f
    conf.configure[Path]("xxx", PathUtil.pathTo("/path/to/nowhere")) shouldBe PathUtil.pathTo("/path/to/nowhere")
    conf.configure[Path]("xxx", PathUtil.pathTo("/path/to/nowhere")) shouldBe PathUtil.pathTo("/path/to/nowhere")
    conf.configure[Cores]("no-cores.here", Cores(50f)) shouldBe Cores(50.0)
    conf.configure[Memory]("i.forgot", Memory("128G")) shouldBe Memory("128g")
  }

  it should "throw an exception for unsupported types" in {
    an[IllegalArgumentException] should be thrownBy conf.configure[Char]("a-char")
    val log = captureLogger(() => {
      an[IllegalArgumentException] should be thrownBy conf.configure[Set[String]]("a-string-set", Set("String"))
    })
    log should include("IllegalArgumentException")
  }

  it should "load a config from a file" in {
    val configPath = Files.createTempFile("config", ".txt")
    configPath.toFile.deleteOnExit()

    val pw = new PrintWriter(Io.toWriter(configPath))
    pw.println(Configuration.Keys.CommandLineName + " = command-line-name")
    pw.println(Configuration.Keys.ColorStatus + " = false")
    pw.close()

    val conf = new Configuration {  override val config : Config = ConfigFactory.parseFile(configPath.toFile) }
    conf.configure[String](Configuration.Keys.CommandLineName) shouldBe "command-line-name"
    conf.configure[Boolean](Configuration.Keys.ColorStatus) shouldBe false
    conf.optionallyConfigure[String](Configuration.Keys.SystemPath) shouldBe 'empty
  }

  it should "find executables" in {
    conf.configureExecutable("some-executable", "n/a") shouldBe PathUtil.pathTo("/does/not/exist")
    conf.configureExecutableFromBinDirectory("some-executable", "exec") shouldBe PathUtil.pathTo("/does/not/exist/exec")
    conf.configureExecutableFromBinDirectory("some-executable", "exec", Some(Paths.get("some", "subdir"))) shouldBe PathUtil.pathTo("/does/not/exist/some/subdir/exec")

    var java = conf.configureExecutable("java.exe", "java")
    java.getFileName.toString shouldBe "java"
    Files.isExecutable(java) shouldBe true

    // If the config key is found, but the value, as a path, does not exist, then fallback to the system path.
    java = conf.configureExecutable("some-executable", "java", mustExist = true)
    java.getFileName.toString shouldBe "java"
    Files.isExecutable(java) shouldBe true

    // the bin directory should not be found, and so should fall back on the system path, and find the java executable
    java = conf.configureExecutableFromBinDirectory("path-does-not-exist", "java")
    java.getFileName.toString shouldBe "java"
    Files.isExecutable(java) shouldBe true
    Configuration.requestedKeys should contain("java.exe")
  }

  it should "thrown an exception when an executable cannot be found" in {
    an[ConfigException] should be thrownBy conf.configureExecutable("some-executable-not-found", "n/a")
    an[ConfigException] should be thrownBy conf.configureExecutableFromBinDirectory("some-bin-dir-not-found", "n/a")
  }
}
