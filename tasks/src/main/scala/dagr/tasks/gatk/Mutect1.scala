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
package dagr.tasks.gatk

import java.nio.file.Path

import dagr.tasks.DagrDef
import DagrDef._

import scala.collection.mutable.ListBuffer

/**
  * Runs Mutect 1 from the separate Mutect/CGA tools distribution
  */
class Mutect1(val tumorBam: PathToBam,
              val normalBam: Option[PathToBam] = None,
              ref: PathToFasta,
              intervals: PathToIntervals,
              val dbsnpVcf: Option[PathToVcf] = None,
              val vcfOutput: PathToVcf,
              val callStatsOutput: Path,
              val tumorSampleName: String = "tumor",
              val normalSampleName: String = "normal",
              val minimumAf: Option[Double] = None,
              val fractionContamination: Double = 0,
              val maxAltAlleleInNormalFraction: Double = 0.03,
              val maxAltAlleleInNormalCount: Int = 5,  // MuTect Default = 2
              val maxAltAlleleInNormalBqSum: Int = 40, // MuTect Default = 40
              val minBaseQuality: Int = 5
             ) extends GatkTask(walker="MuTect", ref=ref, intervals=Some(intervals)) {

  override protected def addWalkerArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("-o",   callStatsOutput)
    buffer.append("-vcf", vcfOutput)
    dbsnpVcf.foreach(vcf => buffer.append("--dbsnp", vcf))
    buffer.append("--input_file:tumor",  tumorBam)
    normalBam.foreach(buffer.append("--input_file:normal", _))
    buffer.append("--tumor_sample_name", tumorSampleName)
    buffer.append("--normal_sample_name", normalSampleName)
    minimumAf.foreach(buffer.append("--minimum_mutation_cell_fraction", _))
    buffer.append("--fraction_contamination", fractionContamination)
    buffer.append("--max_alt_allele_in_normal_fraction", maxAltAlleleInNormalFraction)
    buffer.append("--max_alt_alleles_in_normal_count", maxAltAlleleInNormalCount)
    buffer.append("--max_alt_alleles_in_normal_qscore_sum", maxAltAlleleInNormalBqSum)
    buffer.append("--min_qscore", minBaseQuality)
  }

  /** Use a custom JAR for running Mutect1. */
  override protected def gatkJar: Path = configure[Path]("mutect1.jar")
}
