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

package dagr.api.models

import java.time.Instant

import com.fulcrumgenomics.commons.io.PathUtil
import com.fulcrumgenomics.commons.util.Logger
import dagr.core.DagrDef.TaskId
import dagr.core.exec.ResourceSet
import dagr.core.tasksystem.Task
import dagr.core.tasksystem.Task.{TaskInfoLike, TaskStatus, TimePoint}
import com.fulcrumgenomics.commons.CommonsDef._
import upickle.Js
import upickle.default.{Reader, Writer}

import scala.collection.mutable.ListBuffer


case class TaskInfo
(
  task       : Task,
  name       : String,
  id         : Option[TaskId],
  attempts   : Int,
  script     : Option[FilePath],
  log        : Option[FilePath],
  resources  : Option[ResourceSet],
  exitCode   : Option[Int],
  throwable  : Option[Throwable],
  status     : TaskStatus, // FIXME: when we only have one exec system, this should be type TaskStatus
  timePoints : Traversable[TimePoint],
  statusTime : Instant

) extends TaskInfoLike {
  protected[dagr] def logTaskMessage(logger: Logger) = Unit
}

object TaskInfo {
  import DagrApiDef._

  def apply(info: TaskInfoLike): TaskInfo = {
    TaskInfo(
      task       = info.task,
      name       = info.task.name,
      id         = info.id,
      attempts   = info.attempts,
      script     = info.script,
      log        = info.log,
      resources  = info.resources,
      exitCode   = info.exitCode,
      throwable  = info.throwable,
      status     = info.status,
      timePoints = info.timePoints,
      statusTime = null
    )
  }

  implicit val query2Writer: Writer[TaskInfo] = Writer[TaskInfo] {
    info: TaskInfo =>
      val tuples = ListBuffer[(String, Js.Value)]()

      tuples += (("name", Js.Str(info.name)))
      info.id.foreach { id => tuples += (("id", Js.Num(id.toInt))) }
      tuples += (("attempts", Js.Num(info.attempts)))
      info.script.foreach { s => tuples += (("script", Js.Str(s.toString))) }
      info.log.foreach { l => tuples += (("log", Js.Str(l.toString))) }
      info.resources.foreach { r => tuples += (("resources", fromResourceSet(Some(r)))) }
      info.exitCode.foreach { e => tuples += (("exit_code", Js.Num(e))) }
      info.throwable.foreach { t => tuples += (("throwable", Js.Str(t.getMessage))) }
      tuples += (("status", fromTaskStatus(info.status)))
      tuples += (("time_points", Js.Arr(info.timePoints.map(fromTimePoint).toSeq:_*)))
      tuples += (("status_time", Js.Str(info.statusTime.toString)))

      Js.Obj(tuples:_*)
  }

  implicit val query2Reader: Reader[TaskInfo] = Reader[TaskInfo] {
    case obj: Js.Obj =>
      val names = obj.value.map(_._1).toSet

      def ifContains[T](name: String)(f: => T): Option[T] = if (names.contains(name)) Some(f) else None

      TaskInfo(
        task       = null,
        name       = obj("name").str,
        id         = ifContains("id") { BigInt(obj("id").num.toInt) },
        attempts   = obj("attempts").num.toInt,
        script     = ifContains("script") { PathUtil.pathTo(obj("script").str) },
        log        = ifContains("log") { PathUtil.pathTo(obj("log").str) },
        resources  = ifContains("resources") { toResourceSet(obj("resources")).get },
        exitCode   = ifContains("exit_code") { obj("exit_code").num.toInt },
        throwable  = ifContains("throwable") { new Throwable(obj("throwable").str) },
        status     = toTaskStatus(obj("status")),
        timePoints = obj("time_points").arr.map(toTimePoint),
        statusTime = Instant.parse(obj("status_time").str)
      )
  }
}