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

package dagr.core.util

import java.time.Instant

import com.fulcrumgenomics.commons.io.PathUtil
import com.fulcrumgenomics.commons.util.Logger
import dagr.core.DagrDef.TaskId
import dagr.core.exec.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.Task
import dagr.core.tasksystem.Task.{TaskInfoLike, TaskStatus, TimePoint}
import com.fulcrumgenomics.commons.CommonsDef.FilePath
import upickle.Js

import scala.collection.mutable.ListBuffer

case class TaskInfo
(
  name       : String,
  task       : Task,
  id         : Option[TaskId],
  attempts   : Int,
  script     : Option[FilePath],
  log        : Option[FilePath],
  resources  : Option[ResourceSet],
  exitCode   : Option[Int],
  throwable  : Option[Throwable],
  status     : TaskStatus,
  timePoints : Seq[TimePoint],
  statusTime : Instant,
  dependsOn  : Seq[TaskId], // task that this tasks depends on
  dependents : Seq[TaskId]  // tasks that depend on this task
) extends TaskInfoLike {
  protected[dagr] def logTaskMessage(logger: Logger): Unit = Unit
}

object TaskInfo {

  private def statusFrom(ordinal: Int, expSystem: Boolean): TaskStatus = {
    if (expSystem) {
      dagr.core.execsystem2.TaskStatus.withValue(ordinal)
    }
    else {
      dagr.core.execsystem.TaskStatus.withValue(ordinal)
    }
  }

  def toString(info: TaskInfoLike): String = {
    val expSystem: Boolean =  try {
      info.task._executor.get.statusFrom(dagr.core.execsystem2.TaskStatus.ManuallySucceeded.ordinal)
      true
    } catch {
      case e: Exception => false
    }

    val dependsOn = info.task.tasksDependedOn.map { d =>
      d.taskInfo.id.getOrElse("None").toString
    }
    val dependents = info.task.tasksDependingOnThisTask.map { d =>
      d.taskInfo.id.getOrElse("None").toString
    }
    val timePoints = info.timePoints.map(_.toString).mkString(":")
    val lines = ListBuffer[String]()

    lines += s"exp_exec,$expSystem"
    lines += s"name,${info.task.name}"
    info.id.foreach { id => lines += s"id,$id" }
    lines += s"attempts,${info.attempts}"
    info.script.foreach { script => lines += s"script,$script" }
    info.log.foreach { log => lines += s"log,$log" }
    info.resources.foreach { r => lines += s"resources,${r.cores.toString},${r.memory.prettyString}" }
    info.exitCode.foreach { e => lines += s"exit_code,$e" }
    info.throwable.foreach { t => lines += s"throwable,${t.getMessage}" }
    lines += s"status,${info.status.ordinal}"
    lines += s"time_points,$timePoints"
    lines += s"depends_on,${dependsOn.mkString(":")}"
    lines += s"dependents,${dependents.mkString(":")}"
    lines.mkString("\t")
  }

  def parse(str: String): TaskInfo = {
    val map: Map[String, String] = str.split('\t').map { line =>
      line.split(",", 1).toSeq match {
        case Seq(key, value) => key -> value
      }
    }.toMap

    val expSystem = map("exp_exec").toBoolean

    val name       = map("name")
    val resources  = map.get("resources").map { line =>
      line.split(',').toSeq match {
        case Seq(cores, memory) => new ResourceSet(Cores(cores.toDouble), Memory(memory))
      }
    }
    val status     = statusFrom(map("status").toInt, expSystem)
    val timePoints = map("time_points").split(':').map { v => TimePoint.parse(v, i => statusFrom(i, expSystem)) }
    val dependsOn  = map("depends_on").split(':').map { value => BigInt(value) }
    val dependents = map("dependents").split(":").map { value => BigInt(value) }

    TaskInfo(
      name       = name,
      task       = null,
      id         = map.get("id").map(BigInt(_)),
      attempts   = map("attempts").toInt,
      script     = map.get("script").map(PathUtil.pathTo(_)),
      log        = map.get("log").map(PathUtil.pathTo(_)),
      resources  = resources,
      exitCode   = map.get("exit_code").map(_.toInt),
      throwable  =map.get("throwable").map(new Throwable(_)),
      status     = status,
      timePoints = timePoints,
      statusTime = timePoints.find(_.status.ordinal == status.ordinal).get.instant,
      dependsOn  = dependsOn,
      dependents = dependents
    )
  }

  implicit val taskInfoToWriter: upickle.default.Writer[TaskInfo] = upickle.default.Writer[TaskInfo] { info =>
    Js.Str(toString(info))
  }

  implicit val taskInfoToReader: upickle.default.Reader[TaskInfo] = upickle.default.Reader[TaskInfo] {
    case Js.Str(str) => TaskInfo.parse(str)
  }
}