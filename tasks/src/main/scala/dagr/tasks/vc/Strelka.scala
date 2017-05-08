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

package dagr.tasks.vc

import java.nio.file.Path

import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.config.Configuration
import dagr.core.execsystem.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.{Pipeline, ProcessTask, ShellCommand, VariableResources}
import dagr.tasks.DagrDef._
import dagr.tasks.misc.DeleteFiles
import dagr.tasks.picard.MergeVcfs

object Strelka extends AnyRef with Configuration {
  /* Strelka install directory - users will want access to this to lookup config files. */
  val StrelkaDir = configure[Path]("strelka.dir")
}

/**
 * Runs the Strelka somatic variant caller from Illumina, cleans up after it and merges the results
 * into a single VCF for downstream analysis.
 */
class Strelka(val tumor: PathToBam,
              val normal: PathToBam,
              val configFile: FilePath,
              val ref: PathToFasta,
              val out: PathToVcf,
              val tmp: Option[DirPath],
              val minThreads: Int = 1,
              val maxThreads: Int = 8
             ) extends Pipeline with Configuration with LazyLogging {

  override def build(): Unit = {
    val tmpdir = tmp.getOrElse(out.getParent).resolve("strelka")

    val configStrelka = new ShellCommand(Strelka.StrelkaDir.resolve("bin/configureStrelkaWorkflow.pl").toString,
      "--tumor=" + tumor, "--normal=" + normal, "--ref=" + ref, "--config=" + configFile, "--output-dir=" + tmpdir
    ).withName("ConfigureStrelka")

    // Run strelka with 1G per core and as many cores as can be had
    val runStrelka = new ProcessTask with VariableResources {
      override def args: Seq[Any] = Seq("make", "--directory=" + tmpdir, "-j", this.resources.cores.toInt)

      override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
        (maxThreads to minThreads by -1).toStream
          .flatMap(c => availableResources.subset(Cores(c), memory = Memory(s"${c}G")))
          .headOption
      }
    }.withName("RunStrelka")

    val merge   = new MergeVcfs(in=Seq("indels", "snvs").map(t => tmpdir.resolve("results/passed.somatic." + t + ".vcf")), out=out)
    val cleanup = new DeleteFiles(tmpdir).withName("DeleteStrelkaTmpDir")

    root ==> configStrelka ==> runStrelka ==> merge ==> cleanup
  }
}
