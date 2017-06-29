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

package dagr.webservice

import java.time.Instant

import dagr.core.DagrDef.TaskId
import dagr.core.exec.ResourceSet
import dagr.core.tasksystem.Task.TaskInfoLike
import dagr.tasks.DagrDef.FilePath

import scala.util.matching.Regex

/** Stores the data to be returned by an end-point. Make sure that there exists a protocol and any custom JSON
  * handling specified in [[DagrApiJsonSupport]].
  */
sealed abstract class DagrResponse

case class DagrVersionResponse(id: String) extends DagrResponse

case class DagrTaskInfosResponse(infos: Iterable[TaskInfoLike]) extends DagrResponse

case class DagrTaskScriptResponse(script: Option[FilePath]) extends DagrResponse

case class DagrTaskLogResponse(log: Option[FilePath]) extends DagrResponse

case class DagrTaskInfoResponse(info: TaskInfoLike) extends DagrResponse

case class TaskInfoQuery
(
  name: Seq[String] = Seq.empty, // any name pattern
  status: Seq[String] = Seq.empty, // name
  id: Seq[TaskId] = Seq.empty, // any task ids
  attempts: Seq[Int] = Seq.empty, // any attempt #
  minResources: Option[ResourceSet] = None,
  maxResources: Option[ResourceSet] = None,
  since: Option[Instant] = None, // since using statusTime
  until: Option[Instant] = None, // until using statusTime,
  dependsOn: Seq[TaskId] = Seq.empty,
  dependents: Seq[TaskId] = Seq.empty,
  parent: Option[TaskId] = None, // TODO: include this in the response
  children: Seq[TaskId] = Seq.empty
) {
  // TODO: filter out regexes that match the same stuff?
  private val nameRegexes: Seq[Regex] = this.name.map(_.r)

  def get(infos: Iterable[TaskInfoTracker.Info]): Iterable[TaskInfoLike] = infos
    .filter { info => this.nameRegexes.isEmpty || this.nameRegexes.exists(_.findFirstIn(info.info.task.name).isDefined) }
    .filter { info => this.status.isEmpty || this.status.contains(info.info.status.name)}
    .filter { info => this.id.isEmpty || info.info.id.forall(this.id.contains(_)) }
    .filter { info => this.attempts.isEmpty || this.attempts.contains(info.info.attempts) }
    .filter { info => leq(this.minResources,info.info.resources) }
    .filter { info => geq(this.maxResources,info.info.resources) }
    .filter { info => this.since.forall(s => s.compareTo(info.info.statusTime) <= 0) }
    .filter { info => this.until.forall(u => u.compareTo(info.info.statusTime) <= 0) }
    .filter { info => this.dependsOn.isEmpty || intersects(this.dependsOn,info.info.task.tasksDependedOn.flatMap(_.taskInfo.id).toSeq) }
    .filter { info => this.dependents.isEmpty || intersects(this.dependents,info.info.task.tasksDependingOnThisTask.flatMap(_.taskInfo.id).toSeq) }
    .filter { info => this.parent.forall(pid =>info.parent.id.contains(pid)) }
    .filter { info => this.children.isEmpty || intersects(this.children, info.children.flatMap(_.id)) }
    .map    { info => info.info }

  private def intersects(left: Seq[TaskId], right: Seq[TaskId]): Boolean = {
    left.exists { l => right.contains(l) }
  }

  /** Returns true left is not defined, or if left and right are defined and left is <= right in both cores and memory,
    * false otherwise. */
  private def leq(left: Option[ResourceSet], right: Option[ResourceSet]): Boolean = (left, right) match {
    case (None,  _)         => true
    case (Some(l), Some(r)) => l.cores <= r.cores && l.memory <= r.memory
    case (Some(_), None)    => false
  }

  /** Returns true left is not defined, or if left and right are defined and left is >= right in both cores and memory,
    * false otherwise. */
  private def geq(left: Option[ResourceSet], right: Option[ResourceSet]): Boolean = (left, right) match {
    case (None,  _)         => true
    case (Some(l), Some(r)) => l.cores >= r.cores && l.memory >= r.memory
    case (Some(_), None)    => false
  }
}