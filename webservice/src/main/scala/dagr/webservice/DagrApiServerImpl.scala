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

package dagr.webservice

import dagr.api.DagrApi
import dagr.api.models.{TaskInfo, TaskInfoQuery, TaskInfoResponse}
import dagr.core.DagrDef.TaskId
import dagr.core.exec.ResourceSet

import scala.util.matching.Regex

trait DagrApiServerImpl extends DagrApi {
  def taskInfoTracker: TaskInfoTracker

  def version(): String = DagrApiService._version

  def query(query: TaskInfoQuery): TaskInfoResponse = {
    val nameRegexes: Seq[Regex] = query.name.map(_.r)
    val infos: Seq[TaskInfo] = taskInfoTracker.fullInfo
      .filter { info => nameRegexes.isEmpty || nameRegexes.exists(_.findFirstIn(info.info.task.name).isDefined) }
      .filter { info => query.status.isEmpty || query.status.contains(info.info.status.name)}
      .filter { info => query.id.isEmpty || info.info.id.forall(query.id.contains(_)) }
      .filter { info => query.attempts.isEmpty || query.attempts.contains(info.info.attempts) }
      .filter { info => leq(query.minResources,info.info.resources) }
      .filter { info => geq(query.maxResources,info.info.resources) }
      .filter { info => query.since.forall(s => s.compareTo(info.info.statusTime) <= 0) }
      .filter { info => query.until.forall(u => u.compareTo(info.info.statusTime) <= 0) }
      .filter { info => query.dependsOn.isEmpty || intersects(query.dependsOn,info.info.task.tasksDependedOn.flatMap(_.taskInfo.id).toSeq) }
      .filter { info => query.dependents.isEmpty || intersects(query.dependents,info.info.task.tasksDependingOnThisTask.flatMap(_.taskInfo.id).toSeq) }
      .filter { info => query.parent.forall(pid =>info.parent.id.contains(pid)) }
      .filter { info => query.children.isEmpty || intersects(query.children, info.children.flatMap(_.id)) }
      .map    { info => TaskInfo.apply(info.info) }
      .toSeq
    TaskInfoResponse(infos=infos)
  }

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