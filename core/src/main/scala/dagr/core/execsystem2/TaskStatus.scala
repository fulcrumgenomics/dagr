/*
 * The MIT License
 *
 * Copyright (c) 2017 Fulcrum Genomics LLC
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
 *
 */

package dagr.core.execsystem2

import dagr.api.models.{TaskStatus => RootTaskStatus}
import enumeratum.values.{IntEnum, IntEnumEntry}

import scala.collection.immutable.IndexedSeq

/** The root of all task statuses in [[dagr.core.execsystem2]]. */
sealed abstract class TaskStatus extends IntEnumEntry with RootTaskStatus {
  override def ordinal = this.value
  /** Returns true if this status indicates any type of success, false otherwise. */
  def success: Boolean = this.isInstanceOf[TaskStatus.Succeeded]
  /** Returns true if this status indicates any type of failure, false otherwise. */
  def failure: Boolean = this.isInstanceOf[TaskStatus.Failed]
  /** Returns true if this status indicates the task is executing, false otherwise. */
  def executing: Boolean = this == TaskStatus.Running
}

case object TaskStatus extends IntEnum[TaskStatus] {
  override val values: IndexedSeq[TaskStatus] = findValues

  /** Trait for all statuses prior to submission for execution */
  sealed trait PreSubmission     extends TaskStatus
  case object Pending            extends PreSubmission { val description: String = "Has unmet dependencies";                 val value: Int = 0 }
  case object Queued             extends PreSubmission { val description: String = "Ready for execution";                    val value: Int = 1 }

  /** Trait for all statuses during execution */
  sealed trait Executing         extends TaskStatus
  case object Submitted          extends Executing     { val description: String = "Submitted for execution";                val value: Int = 2 }
  case object Running            extends Executing     { val description: String = "Executing";                              val value: Int = 3 }

  /** Trait for all statuses after execution has completed */
  sealed trait Completed         extends TaskStatus
  sealed trait Failed            extends Completed
  sealed trait Succeeded         extends Completed
  case object Stopped            extends Completed     { val description: String = "Stopped prior to completion";            val value: Int = 4 }
  case object FailedToBuild      extends Failed        { val description: String = "Failed to build the task";               val value: Int = 5 }
  case object FailedSubmission   extends Failed        { val description: String = "Could not be submitted to the executor"; val value: Int = 6 }
  case object FailedExecution    extends Failed        { val description: String = "Failed during execution";                val value: Int = 7 }
  case object FailedOnComplete   extends Failed        { val description: String = "Failed during the onComplete callback";  val value: Int = 8 }
  case object FailedUnknown      extends Failed        { val description: String = "Failed for unknown reasons";             val value: Int = 9 }
  case object SucceededExecution extends Succeeded     { val description: String = "Succeeded execution";                    val value: Int = 10 }
  case object ManuallySucceeded  extends Succeeded     { val description: String = "Manually marked as succeeded";           val value: Int = 11 }
}
