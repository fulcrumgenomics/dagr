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

import java.nio.file.{Path, Paths}
import java.time.Instant

import com.sun.xml.internal.ws.encoding.soap.DeserializationException
import dagr.core.DagrDef._
import dagr.core.exec.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.Task.{TaskInfoLike, TaskStatus, TimePoint}
import spray.json._

import scala.language.implicitConversions

/** Methods for formatting custom types in JSON. */
trait DagrApiJsonSupport extends DefaultJsonProtocol {
  implicit val dagrVersionResponseProtocol: RootJsonFormat[DagrVersionResponse]       = jsonFormat1(DagrVersionResponse)
  implicit val dagrStatusResponseProtocol: RootJsonFormat[DagrStatusResponse]         = jsonFormat1(DagrStatusResponse)
  implicit val dagrTaskScriptResponseProtocol: RootJsonFormat[DagrTaskScriptResponse] = jsonFormat1(DagrTaskScriptResponse)
  implicit val dagrTaskLogResponseProtocol: RootJsonFormat[DagrTaskLogResponse]       = jsonFormat1(DagrTaskLogResponse)
  implicit val dagrTaskInfoResponseProtocol: RootJsonFormat[DagrTaskInfoResponse]     = jsonFormat1(DagrTaskInfoResponse)

  def taskInfoTracker: TaskInfoTracker

  implicit object TaskStatusFormat extends RootJsonFormat[TaskStatus] {
    override def write(status: TaskStatus) = JsString(status.ordinal.toString)

    override def read(json: JsValue): TaskStatus = json match {
      case JsString(value) => taskInfoTracker.statusFrom(value.toInt)
      case _ => throw new DeserializationException("only string supported")
    }
  }

  implicit object PathFormat extends RootJsonFormat[Path] {
    override def write(path: Path) = JsString(path.toString)

    override def read(json: JsValue): Path = json match {
      case JsString(value) => Paths.get(value)
      case _ => throw new DeserializationException("only string supported")
    }
  }

  implicit object ResoureSetFormat extends RootJsonFormat[ResourceSet] {
    override def write(resourceSet: ResourceSet): JsObject = {
      var map = Map.empty[String, JsValue]
      map += ("cores"  -> JsString(resourceSet.cores.toString))
      map += ("memory" -> JsString(resourceSet.memory.prettyString))
      JsObject(map)
    }

    override def read(json: JsValue): ResourceSet = {
      val jsObject = json.asJsObject
      ResourceSet(
        cores  = Cores(jsObject.fields("cores").convertTo[Float]),
        memory = Memory(jsObject.fields("memory").convertTo[String])
      )
    }
  }

  implicit object InstantFormat extends RootJsonFormat[Instant] {
    override def write(timestamp: Instant) = JsString(timestamp.toString)

    override def read(json: JsValue): Instant = json match {
      case JsString(value) => Instant.parse(value)
      case _ => throw new DeserializationException("only string supported")
    }
  }

  implicit object TimePointFormat extends RootJsonFormat[TimePoint] {
    override def write(timePoint: TimePoint) = JsString(timePoint.toString)

    override def read(json: JsValue): TimePoint = json match {
      case JsString(value) => TimePoint.parse(value, taskInfoTracker.statusFrom)
      case _ => throw new DeserializationException("only string supported")
    }
  }

  implicit object TaskInfoLikeFormat extends RootJsonFormat[TaskInfoLike] {
    override def write(info: TaskInfoLike): JsObject = {
      var map = Map.empty[String, JsValue]
      val dependsOn = info.task.tasksDependedOn.map { d =>
        s"${d.taskInfo.id.getOrElse("None")},${d.name},${d.taskInfo.status.name},${d.taskInfo.status.description}"
      }
      val dependents = info.task.tasksDependingOnThisTask.map { d =>
        s"${d.taskInfo.id.getOrElse("None")},${d.name},${d.taskInfo.status.name},${d.taskInfo.status.description}"
      }

      map += ("name" -> JsString(info.task.name))
      info.id.foreach { id => map += ("id" -> id.toJson) }
      map += ("attempts" -> info.attempts.toJson)
      info.script.foreach { script => map += ("script" -> script.toJson) }
      info.log.foreach { log => map += ("log" -> log.toJson) }
      info.resources.foreach { r => map += ("resources" -> r.toJson) }
      info.exitCode.foreach { e => map += ("exit_code" -> e.toJson) }
      info.throwable.foreach { t => map += ("throwable" -> t.getMessage.toJson) }
      map += ("status" -> info.status.ordinal.toJson)
      map += ("time_points" -> info.timePoints.toList.toJson)
      map += ("depends_on" -> dependsOn.toList.toJson)
      map += ("dependents" -> dependents.toList.toJson)

      JsObject(map)
    }

    override def read(json: JsValue): TaskInfoLike = {
      val jsObject = json.asJsObject

      val name = jsObject.fields("name").convertTo[String]
      val taskId = jsObject.fields("id").convertTo[TaskId]
      /*
      val attempts = jsObject.fields("attempts").convertTo[Int]
      val script = pathOrNone(jsObject, "script")
      val log = pathOrNone(jsObject, "log")
      val resources = jsObject.fields("resources").convertTo[ResourceSet]
      val exitCode = jsObject.fields("exit_code").convertTo[Int]
      val throwable = new Throwable(jsObject.fields("throwable").convertTo[String]) // TODO: get the original class?
      val status = jsObject.fields("status").convertTo[Int] // NB: the ordinal
      val timePoints = jsObject.fields("time_points").convertTo[Traversable[TimePoint]]
      */

      val info = taskInfoTracker.info(taskId).getOrElse {
        throw new IllegalArgumentException(s"Could not retrieve task with id '$taskId'")
      }

      if (info.task.name.compareTo(name) != 0) throw new IllegalStateException(s"Task names differ! '$name' != '${info.task.name}'")

      info
    }
  }

  /*
  private def pathOrNone(json: JsObject, key: String): Option[FilePath] = {
    if (json.getFields(key).nonEmpty) json.fields(key).convertTo[Option[FilePath]] else None
  }

  private def instantOrNone(json: JsObject, key: String): Option[Instant] = {
    if (json.getFields(key).nonEmpty) json.fields(key).convertTo[Option[Instant]] else None
  }
  */
}