/*
 * The MIT License
 *
 * Copyright (c) 2015 Fulcrum Genomics LLC
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
package dagr.tasks.picard

import java.nio.file.Path

import dagr.tasks.DagrDef
import DagrDef.{PathToBam, PathToFasta}
import picard.analysis.CollectMultipleMetrics.Program

import scala.collection.mutable.ListBuffer

class CollectMultipleMetrics(override val in: PathToBam,
                             override val prefix: Option[Path] = None,
                             ref: PathToFasta,
                             assumeSorted: Boolean = true,
                             programs: Seq[Program] = Seq(
                               Program.CollectAlignmentSummaryMetrics,
                               Program.CollectBaseDistributionByCycle,
                               Program.CollectInsertSizeMetrics,
                               Program.CollectQualityYieldMetrics,
                               Program.CollectSequencingArtifactMetrics,
                               Program.MeanQualityByCycle,
                               Program.QualityScoreDistribution
                             ),
                             fileExtension: Option[String] = Some("." + PicardOutput.Text.toString))
  extends PicardTask with PicardMetricsTask {

  // Since we do not actually want any extensions, the tool will do that itself
  override def metricsExtension: String = ""

  /** Build method that Picard tasks should override instead of build(). */
  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("I=" + in)
    buffer.append("O=" + pathPrefix)
    buffer.append("R=" + ref)
    buffer.append("AS=" + assumeSorted)
    buffer.append("PROGRAM=null")
    programs.foreach(program => buffer.append("PROGRAM=" + program.name()))
    fileExtension.foreach(ext => buffer.append("FILE_EXTENSION=" + ext))
  }
}
