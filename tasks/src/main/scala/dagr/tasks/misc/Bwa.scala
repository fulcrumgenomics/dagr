package dagr.tasks.misc

import dagr.core.config.Configuration

/**
  * Constants and defaults that are used across types of bwa invocation
  */
object Bwa extends Configuration {
  val BwaExecutableConfigKey: String = "bwa.executable"

  def findBwa = configureExecutable(BwaExecutableConfigKey, "bwa")
}
