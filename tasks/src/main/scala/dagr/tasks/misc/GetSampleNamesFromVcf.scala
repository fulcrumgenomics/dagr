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

package dagr.tasks.misc

import dagr.core.tasksystem.SimpleInJvmTask
import dagr.tasks.DagrDef
import DagrDef.PathToVcf
import htsjdk.samtools.util.CloserUtil
import htsjdk.variant.vcf.{VCFFileReader, VCFHeader}
import scala.collection.mutable

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/** Gets the sample names from a VCF */
class GetSampleNamesFromVcf(val vcf: PathToVcf) extends SimpleInJvmTask {
  name = "GetSampleNamesFromVcf"
  private val _sampleNames: mutable.ListBuffer[String] = new ListBuffer[String]()
  def sampleNames: List[String] = this._sampleNames.toList

  override def run(): Unit = {
    val reader: VCFFileReader = new VCFFileReader(vcf.toFile, false)
    val header: VCFHeader = reader.getFileHeader
    this._sampleNames ++= header.getSampleNamesInOrder.toList
    CloserUtil.close(reader)
  }
}
