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

import com.fulcrumgenomics.commons.io.PathUtil
import dagr.tasks.DagrDef
import DagrDef.{PathPrefix, PathToBam}

object PicardOutput extends Enumeration {
  val Text = Value("txt")
  val Pdf  = Value("pdf")
}

/** Simple trait that requires knowing where a metrics file is for a Picard metric generating tool. */
trait PicardMetricsTask {

  /** The path to the input BAM */
  def in: PathToBam

  /** The optional path to the prefix of the metrics file */
  def prefix: Option[PathPrefix] = None

  /** Method that should be used to fetch the file prefix, instead of accessing `prefix` directly. */
  def pathPrefix: PathPrefix = prefix getOrElse PathUtil.removeExtension(in)

  /** Gets the metrics file path, assuming that the program generates a single metrics file with a known extension.  */
  def metricsFile: Path = {
    metricsFile(metricsExtension, PicardOutput.Text)
  }

  /**
   *  Build a path to a metrics file (text of PDF) using the appropriate prefix derived from either the
   * supplied prefix, or the input file name.  Useful when a program produces multiple outputs, as in
   * the case of CollectGcBiasMetrics.
    */
  def metricsFile(extension: String, kind: PicardOutput.Value) : Path = {
    PathUtil.pathTo(s"${pathPrefix}${extension}.${kind}")
  }

  /** All Picard metric generating tools should define their own metrics extension. */
  def metricsExtension: String
}
