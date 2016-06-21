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


package dagr.tasks.parallel

import java.nio.file.Path

import dagr.core.tasksystem.{Linker, Task, Pipeline}
import dagr.tasks.misc.DeleteFiles

object PathBasedScatterGatherPipeline {

  /** A wrapper for the scatter task. */
  private class ScatterPathAdapterTask(input: Path,
                                       scatterTaskGenerator: Path => ScatterTask[Path, Path],
                                       deleteIntermediates: Boolean = true
                                       ) extends ScatterTask[Path, Path] {
    def getTasks: Traversable[_ <: Task] = {
      val scatterTask = scatterTaskGenerator(input)
      // NB: this links to itself, so we can't have this class extend `Pipeline`.
      val linker = Linker(from=scatterTask, to=this)((from, to) => { to._gatheredOutput = Some(from.gatheredOutput) })
      scatterTask ==> linker
      if (deleteIntermediates) {
        val deleteIntermediatesTask = new DeleteFiles(input)
        scatterTask ==> deleteIntermediatesTask
        List(scatterTask, linker, deleteIntermediatesTask)
      }
      else List(scatterTask, linker)
    }
  }

  /** A wrapper for the gather task. */
  private class GatherPathAdapterTask(inputs: Seq[Path],
                                      output: Path,
                                      gatherTaskGenerator: (Seq[Path], Path) => GatherTask[Path],
                                      deleteIntermediates: Boolean = true) extends Pipeline with GatherTask[Path] {
    def gatheredOutput: Path = output
    def build(): Unit = {
      val gatherTask = gatherTaskGenerator(inputs, output)
      root ==> gatherTask
      if (deleteIntermediates) {
        val deleteIntermediatesTask = new DeleteFiles(inputs:_*)
        gatherTask ==> deleteIntermediatesTask
      }
    }
  }
}

/** A Scatter Gather Pipeline that operates on paths.  The input, intermediates, and output are all Paths.
  *
  * The constructor requires methods to generate the various task types.  The first path to the split input and
  * scatter task are the temporary directory in which to operate, and the second argument is the input path.  The
  * gather task takes a sequence of output paths to gather and the final output path.
  */
class PathBasedScatterGatherPipeline
(
  val domain: Path,
  output: Path,
  splitInputPathTaskGenerator: Option[Path] => Path => SplitInputTask[Path, Path],
  scatterTaskGenerator: Option[Path] => Path => ScatterTask[Path, Path],
  gatherTaskGenerator: (Seq[Path], Path) => GatherTask[Path],
  deleteIntermediates: Boolean = true,
  tmpDirectory: Option[Path]
) extends ScatterGatherPipeline[Path, Path, Path] {
  import PathBasedScatterGatherPipeline._

  protected def splitDomainTask(input: Path): SplitInputTask[Path, Path] = splitInputPathTaskGenerator(tmpDirectory)(input)

  protected def scatterTask(intermediate: Path): ScatterTask[Path, Path] = {
    new ScatterPathAdapterTask(input=intermediate, scatterTaskGenerator=scatterTaskGenerator(tmpDirectory), deleteIntermediates=deleteIntermediates)
  }

  protected def gatherTask(outputs: Iterable[Path]): GatherTask[Path] = {
    new GatherPathAdapterTask(inputs=outputs.toSeq, output=output, gatherTaskGenerator=gatherTaskGenerator, deleteIntermediates=deleteIntermediates)
  }
}

// TODO
// Second class:
// - takes in a Path (ex. fasta), produces a Type (ex. region string), for each instance of Type produces a Path (ex. BAM), gathers the Path(s) (ex. VCF)
// - basically input and output are paths, but the intermediate is a type...
// Ideas:
// - It would be nice if the various Path types were "typed", for example Fasta->Bam->Vcf
// - The type of file as input to the gather may be different than the final output type.  For example,
//     BAM -> Metric Files -> Report PDF