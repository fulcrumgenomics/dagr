/*
 * The MIT License
 *
 * Copyright (c) 2019 Fulcrum Genomics LLC
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

import dagr.tasks.DagrDef.{DirPath, PathToFasta, PathToIntervals}
import picard.util.BaitDesigner.DesignStrategy

import scala.collection.mutable.ListBuffer

/** Task to run Picard's BaitDesigner. */
class BaitDesigner(
  val targets: PathToIntervals,
  val designName: String,
  val ref: PathToFasta,
  val outputDirectory: DirPath,
  val leftPrimer: Option[String] = None,
  val rightPrimer: Option[String] = None,
  val designStrategy: Option[DesignStrategy] = None,
  val baitSize: Option[Int] = None,
  val minimumBaitsPerTarget: Option[Int] = None,
  val baitOffset: Option[Int] = None,
  val padding: Option[Int] = None,
  val repeatTolerance: Option[Int] = None,
  val poolSize: Option[Int] = None,
  val fillPools: Option[Boolean] = None,
  val designOnTargetStrand: Option[Boolean] = None,
  val mergeNearbyTargets: Option[Boolean] = None,
  val outputAgilentFiles: Option[Boolean] = None
  ) extends PicardTask {

  override protected def addPicardArgs(buffer: ListBuffer[Any]): Unit = {
    buffer.append("TARGETS=" + targets)
    buffer.append("DESIGN_NAME=" + designName)
    buffer.append("REFERENCE_SEQUENCE=" + ref)
    buffer.append("OUTPUT_DIRECTORY=" + outputDirectory)
    leftPrimer.foreach(primer           => buffer.append("LEFT_PRIMER=" + primer))
    rightPrimer.foreach(primer          => buffer.append("RIGHT_PRIMER=" + primer))
    designStrategy.foreach(strategy     => buffer.append("DESIGN_STRATEGY=" + strategy))
    baitSize.foreach(size               => buffer.append("BAIT_SIZE=" + size))
    minimumBaitsPerTarget.foreach(min   => buffer.append("MINIMUM_BAITS_PER_TARGET=" + min))
    baitOffset.foreach(offset           => buffer.append("BAIT_OFFSET=" + offset))
    padding.foreach(pad                 => buffer.append("PADDING=" + pad))
    repeatTolerance.foreach(tol         => buffer.append("REPEAT_TOLERANCE=" + tol))
    poolSize.foreach(pool               => buffer.append("POOL_SIZE=" + pool))
    fillPools.foreach(fill              => buffer.append("FILL_POOLS=" + fill))
    designOnTargetStrand.foreach(target => buffer.append("DESIGN_ON_TARGET_STRAND=" + target))
    mergeNearbyTargets.foreach(merge    => buffer.append("MERGE_NEARBY_TARGETS=" + merge))
    outputAgilentFiles.foreach(agilent  => buffer.append("OUTPUT_AGILENT_FILES=" + agilent))
  }
}