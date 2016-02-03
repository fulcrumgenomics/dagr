package dagr.tasks.misc

import dagr.core.tasksystem.ShellCommand
import dagr.tasks.PathToVcf
import dagr.core.config.Configuration

/** Tabix (https://github.com/samtools/htslib) */
object Tabix extends Configuration {
  val TabixExecutableConfigKey = "tabix.executable"

  def findTabix = configureExecutable(TabixExecutableConfigKey, "tabix")
}

/** Indexes a VCF.gz using tabix */
class IndexVcfGz(val vcf: PathToVcf)
  extends ShellCommand(Tabix.findTabix.toAbsolutePath.toString, "-p", "vcf", vcf.toAbsolutePath.toString)
