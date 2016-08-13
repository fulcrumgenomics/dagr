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

import java.nio.file.{Files, Path}

import dagr.core.tasksystem.SimpleInJvmTask
import dagr.tasks.DagrDef.{DirPath, PathToFasta, PathToIntervals}
import dagr.tasks.ScatterGather.Partitioner
import htsjdk.samtools.SAMFileHeader
import htsjdk.samtools.reference.IndexedFastaSequenceFile
import htsjdk.samtools.util.{CloserUtil, Interval, IntervalList}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer


/** Creates a set of interval list files given a sequence dictionary.
  *
  * If no intervals are given, it will create one or more interval list files per reference sequence.
  *
  * Each input interval will be broken up into non-overlapping regions of at most `maxBasesPerScatter` size.  Each
  * output interval list will cover at most `maxBasesPerScatter` non-overlapping bases.
  */
class SplitIntervalsForCallingRegions(ref: PathToFasta,
                                      intervals: Option[PathToIntervals],
                                      output: Option[DirPath],
                                      maxBasesPerScatter: Int = 25000000,
                                      prefix: String = "split_interval",
                                      suffix: String = ".interval_list"
                                     ) extends SimpleInJvmTask with Partitioner[PathToIntervals] {
  requires(1, "1g")
  withName("SplitIntervalsForCallingRegions")

  private var intervalLists : Option[Seq[PathToIntervals]] = None

  override def partitions: Option[Seq[PathToIntervals]] = this.intervalLists

  private def writeIntervals(theIntervals: Seq[Interval], idx: Int, header: SAMFileHeader): Path = {
    val pre = s"$prefix.$idx"
    val out = output match {
      case Some(tmpDir) => Files.createTempFile(tmpDir, pre, suffix)
      case None => Files.createTempFile(pre, suffix)
    }
    val intervalList = new IntervalList(header)
    intervalList.addall(theIntervals)
    intervalList.write(out.toFile)
    out
  }

  def run(): Unit = {
    val outputs = new ListBuffer[PathToIntervals]()
    val header = new SAMFileHeader()
    val seqIntervals = intervals match {
      case Some(theIntervals) =>
        val intervalList = IntervalList.fromFile(theIntervals.toFile)
        header.setSequenceDictionary(intervalList.getHeader.getSequenceDictionary)
        intervalList.getIntervals.toSeq
      case None =>
        // read in the sequence dictionary
        val refReader = new IndexedFastaSequenceFile(ref)
        val dict = refReader.getSequenceDictionary
        header.setSequenceDictionary(dict)
        CloserUtil.close(refReader)
        // create an interval per sequence
        dict.getSequences.map { seq =>
          new Interval(seq.getSequenceName, 1, seq.getSequenceLength, false, seq.getSequenceName)
        }.toSeq
    }
    if (seqIntervals.isEmpty) throw new IllegalStateException("No sequence intervals to process")

    // break them up based on the region size
    val brokenUpIntervals = IntervalList.breakIntervalsAtBandMultiples(seqIntervals, maxBasesPerScatter)
    if (brokenUpIntervals.isEmpty) throw new IllegalStateException("No broken up intervals to process")

    // group the intervals such that the total # of bases covered is not greater than `maxBasesPerScatter`.  This can
    // occur if the input intervals are much smaller than `maxBasesPerScatter`.
    val intervalBuffer = ListBuffer[Interval]()
    var size = 0
    var idx = 0
    for (interval <- brokenUpIntervals) {
      // check if adding one more interval will cause us to be too large.  If so output the interval list and clear it.
      if (maxBasesPerScatter < size + interval.length() && intervalBuffer.nonEmpty) {
        outputs.append(writeIntervals(theIntervals=intervalBuffer, idx=idx, header=header))
        size = 0
        idx += 1
        intervalBuffer.clear()
      }
      intervalBuffer.append(interval)
      size += interval.length()
    }
    if (intervalBuffer.nonEmpty) outputs.append(writeIntervals(theIntervals=intervalBuffer, idx=idx, header=header))
    this.intervalLists = Some(outputs)
  }
}
