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

import dagr.tasks.DagrDef
import DagrDef._

import scala.collection.mutable.ListBuffer


object GenotypeGvcfs {
  /** Constructs a GenotypeGvcfs for genotyping multiple samples concurrently. */
  def apply(ref: PathToFasta, intervals: Option[PathToIntervals], gvcfs: Seq[PathToVcf], vcf: PathToVcf, dbSnpVcf: Option[PathToVcf]) : GenotypeGvcfs = {
    new GenotypeGvcfs(ref, intervals, gvcfs, vcf, dbSnpVcf)
  }

  /** Constructs a GenotypeGvcfs for genotyping a single sample. */
  def apply(ref: PathToFasta, intervals: Option[PathToIntervals], gvcf: PathToVcf, vcf: PathToVcf, dbSnpVcf: Option[PathToVcf]) : GenotypeGvcfs = {
    new GenotypeGvcfs(ref, intervals, Seq(gvcf), vcf, dbSnpVcf)
  }
}


/** Genotypes one or more GVCFs concurrently. */
class GenotypeGvcfs private (ref: PathToFasta,
                             intervals: Option[PathToIntervals],
                             val gvcfs: Seq[PathToVcf],
                             val vcf: PathToVcf,
                             val dbSnpVcf: Option[PathToVcf] = None)
  extends GatkTask("GenotypeGVCFs", ref, intervals=intervals) {

  require(gvcfs.length == 1 || gatkMajorVersion < 4, "GenotypeGVCFs only supports one GVCF at a time with GATK version 4+.")

  override protected def addWalkerArgs(buffer: ListBuffer[Any]): Unit = {
    // Args that are common to all versions
    dbSnpVcf.foreach(v => buffer.append("--dbsnp", v.toAbsolutePath.toString))
    gvcfs.foreach(gvcf => buffer.append("-V", gvcf.toAbsolutePath.toString))

    gatkMajorVersion match {
      case n if n < 4  =>
        buffer.append("--out", vcf.toAbsolutePath.toString)
      case n if n >= 4 =>
        buffer.append("--output", vcf.toAbsolutePath.toString)
    }
  }
}
