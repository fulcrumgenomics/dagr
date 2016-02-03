package dagr.tasks.misc

import java.nio.file.{Paths, Path}

import dagr.core.config.{Configuration, DagrConfig}

/**
  * Constants and defaults that are used across types of bwa invocation
  */
object Bwa extends Configuration {
  val BwaExecutableConfigKey: String = "bwa.executable"

  def findBwa = configureExecutable(BwaExecutableConfigKey, "bwa")
}
