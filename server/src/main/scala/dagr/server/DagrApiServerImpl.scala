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

package dagr.server

import java.nio.file.{Files, Path}

import dagr.api.DagrApi.TaskId
import dagr.api.DagrStatusApi
import dagr.api.DagrStatusApi.{StatusResponse, TaskStatusResponse}
import dagr.api.models._
import dagr.api.models.tasksystem.TaskStatus
import dagr.api.models.util.ResourceSet
import dagr.core.tasksystem.Task.{TaskInfo => TaskSystemTaskInfo}

import scala.util.matching.Regex

object DagrApiConversions {
  // NB: does not copy parents/children/dependsOn/dependents
  implicit def toTaskInfo(info: TaskSystemTaskInfo, script: Boolean = false, log: Boolean = false): TaskStatusResponse = {
    TaskStatusResponse(
      name           = info.task.name,
      id             = info.id,
      attempts       = info.attempts,
      script         = info.script,
      log            = info.log,
      resources      = info.resources.map(r => ResourceSet(r.cores.value, r.memory.value)),
      exitCode       = info.exitCode,
      throwable      = info.throwable,
      status         = info.status,
      timePoints     = info.timePoints,
      statusTime     = info.statusTime,
      scriptContents = if (script) toContents(info.script) else None,
      logContents    = if (log) toContents(info.log) else None
    )
  }

  private def readLines(path: Path): Seq[String] = {
    val src = scala.io.Source.fromFile(path.toFile)
    val lines = src.getLines().toList
    require(src.isEmpty)
    src.close()
    lines
  }

  implicit def toContents(path: Option[Path]): Option[String] = {
    path.flatMap { p =>
      if (Files.exists(p)) Some(readLines(p).mkString("\n").toString) else None
    }
  }

  implicit def toTaskStatus(in: TaskStatus): TaskStatus = {
    new TaskStatus {
      override val name: String        = in.name
      override val ordinal: Int        = in.ordinal
      override val success: Boolean    = in.success
      override val failure: Boolean    = in.failure
      override val executing: Boolean  = in.executing
      override val description: String = in.description
    }
  }
}

trait DagrApiServerImpl extends DagrStatusApi {
  import DagrApiConversions._

  def taskInfoTracker: TaskInfoTracker

  def log(id: TaskId): String = {
    taskInfoTracker.info(id)
      .flatMap(info => toContents(info.log))
      .getOrElse { s"No log defined for task with id '$id'"}
  }

  def script(id: TaskId): String = {
    taskInfoTracker.info(id)
      .flatMap(info => toContents(info.script))
      .getOrElse { s"No script defined for task with id '$id'"}
  }

  def report(): String = taskInfoTracker.report.mkString("\n")

  def statuses(): Seq[TaskStatus] = taskInfoTracker.statuses.map(DagrApiConversions.toTaskStatus)

  def status(name: Seq[String] = Seq.empty, // any name pattern
             status: Seq[String] = Seq.empty, // name
             id: Seq[TaskId] = Seq.empty, // any task ids
             attempts: Seq[Int] = Seq.empty, // any attempt #
             minResources: Option[ResourceSet] = None,
             maxResources: Option[ResourceSet] = None,
             since: Option[String] = None, // since using statusTime
             until: Option[String] = None, // until using statusTime,
             dependsOn: Seq[TaskId] = Seq.empty,
             dependents: Seq[TaskId] = Seq.empty,
             parent: Option[TaskId] = None, // TODO: include this in the response
             children: Seq[TaskId] = Seq.empty,
             script: Boolean = false,
             log: Boolean = false,
             report: Boolean = false): StatusResponse = {
    val nameRegexes: Seq[Regex] = name.map(_.r)
    val infos: Seq[TaskStatusResponse] = taskInfoTracker.fullInfo
      .filter { info => nameRegexes.isEmpty || nameRegexes.exists(_.findFirstIn(info.info.task.name).isDefined) }
      .filter { info => status.isEmpty || status.contains(info.info.status.name)}
      .filter { info => id.isEmpty || info.info.id.forall(id.contains(_)) }
      .filter { info => attempts.isEmpty || attempts.contains(info.info.attempts) }
      .filter { info => leq(minResources, info.info.resources) }
      .filter { info => geq(maxResources, info.info.resources) }
      // FIXME
      //.filter { info => since.forall(s => s.compareTo(info.info.statusTime) <= 0) }
      //.filter { info => until.forall(u => u.compareTo(info.info.statusTime) <= 0) }
      .filter { info => dependsOn.isEmpty || intersects(dependsOn,info.info.task.tasksDependedOn.flatMap(_.taskInfo.id).toSeq) }
      .filter { info => dependents.isEmpty || intersects(dependents,info.info.task.tasksDependingOnThisTask.flatMap(_.taskInfo.id).toSeq) }
      .filter { info => parent.forall(pid =>info.parent.id.contains(pid)) }
      .filter { info => children.isEmpty || intersects(children, info.children.flatMap(_.id)) }
      .map    { info =>
        toTaskInfo(info.info, script=script, log=log).copy(
          dependsOn  = info.info.task.tasksDependedOn.flatMap(_.taskInfo.id).toSeq,
          dependents = info.info.task.tasksDependingOnThisTask.flatMap(_.taskInfo.id).toSeq,
          parent     = info.parent.id,
          children   = info.children.flatMap(_.id) // could squash some children
        )
      }
      .toSeq
    StatusResponse(infos=infos, report = if (report) Some(this.report()) else None)
  }

  private def intersects(left: Seq[TaskId], right: Seq[TaskId]): Boolean = {
    left.exists { l => right.contains(l) }
  }

  /** Returns true left is not defined, or if left and right are defined and left is <= right in both cores and memory,
    * false otherwise. */
  private def leq(left: Option[ResourceSet], right: Option[ResourceSet]): Boolean = (left, right) match {
    case (None,  _)         => true
    case (Some(l), Some(r)) => leqExec(l, r)
    case (Some(_), None)    => false
  }

  /** Returns true left is not defined, or if left and right are defined and left is >= right in both cores and memory,
    * false otherwise. */
  private def geq(left: Option[ResourceSet], right: Option[ResourceSet]): Boolean = (left, right) match {
    case (None,  _)         => true
    case (Some(l), Some(r)) => geqExec(l, r)
    case (Some(_), None)    => false
  }

  private def leqExec(l: ResourceSet, r: ResourceSet): Boolean = {
    l.cores >= r.cores && l.memory >= r.memory
  }

  private def geqExec(l: ResourceSet, r: ResourceSet): Boolean = {
    l.cores <= r.cores && l.memory <= r.memory
  }
}
