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
package dagr.tasks.picard

import dagr.core.tasksystem.Pipe
import dagr.tasks.DataTypes.Vcf
import dagr.tasks.PathToVcf

import scala.collection.mutable.ListBuffer

/**
  * Run's Picard's FilterVcf tool to provide hard filtering of variants.
  */
class FilterVcf(val in: PathToVcf,
                val out: PathToVcf,
                val minHetAlleleBalance: Double = 0.2,
                val minDepth: Int = 0,
                val minQd: Double = 6.0,
                val maxFisherStrandValue: Double = 50,
                val minGenotypeQuality: Int = 20)
  extends PicardTask with Pipe[Vcf,Vcf] {

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("I=" + in.toAbsolutePath)
    buffer.append("O=" + out.toAbsolutePath)
    buffer.append("MIN_AB=" + minHetAlleleBalance)
    buffer.append("MIN_DP=" + minDepth)
    buffer.append("MIN_GQ=" + minGenotypeQuality)
    buffer.append("MAX_FS=" + maxFisherStrandValue)
    buffer.append("MIN_QD=" + minQd)
  }
}
