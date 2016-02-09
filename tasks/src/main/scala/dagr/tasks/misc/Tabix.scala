package dagr.tasks.misc

import dagr.core.config.Configuration
import dagr.core.tasksystem.ShellCommand
import dagr.tasks.PathToVcf
import java.nio.file.Path

/** Tabix (https://github.com/samtools/htslib) */
object Tabix extends Configuration {
  val TabixExecutableConfigKey = "tabix.executable"

  def findTabix: Path = configureExecutable(TabixExecutableConfigKey, "tabix")
}

/** Indexes a VCF.gz using tabix */
class IndexVcfGz(val vcf: PathToVcf)
  extends ShellCommand(Tabix.findTabix.toAbsolutePath.toString, "-p", "vcf", vcf.toAbsolutePath.toString)
