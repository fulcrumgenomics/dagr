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

package dagr.core.webservice

import java.nio.file.{Path, Paths}

import com.sun.xml.internal.ws.encoding.soap.DeserializationException
import dagr.core.execsystem._
import dagr.core.execsystem.TaskStatus._
import dagr.core.DagrDef._
import java.time.Instant

import spray.json._
import scala.language.implicitConversions


/** Methods for formatting custom types in JSON. */
object DagrApiJsonSupport extends DefaultJsonProtocol {
  implicit val dagrVersionResponseProtocol = jsonFormat1(DagrVersionResponse)
  implicit val dagrStatusResponseProtocol = jsonFormat1(DagrStatusResponse)
  implicit val dagrTaskScriptResponseProtocol = jsonFormat1(DagrTaskScriptResponse)
  implicit val dagrTaskLogResponseProtocol = jsonFormat1(DagrTaskLogResponse)
  implicit val dagrTaskInfoResponseProtocol = jsonFormat1(DagrTaskInfoResponse)

  var taskManager: TaskManagerLike = null

  implicit object TaskStatusFormat extends RootJsonFormat[TaskStatus] {
    override def write(status: TaskStatus) = JsString(status.toString)

    override def read(json: JsValue): TaskStatus = json match {
      case JsString(value) => TaskStatus.withName(value)
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
    override def write(resourceSet: ResourceSet) = {
      var map = Map.empty[String, JsValue]
      map += ("cores" -> JsNumber(resourceSet.cores.value))
      map += ("memory" -> JsNumber(resourceSet.memory.value))
      JsObject(map)
    }

    override def read(json: JsValue): ResourceSet = {
      val jsObject = json.asJsObject
      ResourceSet(
        cores = jsObject.fields("cores").convertTo[Float],
        memory = jsObject.fields("memory").convertTo[Long]
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

  implicit object TaskExecutionInfoFormat extends RootJsonFormat[TaskExecutionInfo] {
    override def write(info: TaskExecutionInfo) = {
      var map = Map.empty[String, JsValue]
      map += ("name" -> JsString(info.task.name))
      map += ("taskId" -> JsNumber(info.taskId))
      map += ("status" -> info.status.toJson)
      map += ("script" -> info.script.toJson)
      map += ("logFile" -> info.logFile.toJson)
      if (info.submissionDate.nonEmpty) map += ("submissionDate" -> info.submissionDate.get.toJson)
      map += ("resources" -> info.resources.toJson)
      if (info.startDate.nonEmpty) map += ("startDate" -> info.startDate.get.toJson)
      if (info.endDate.nonEmpty) map += ("endDate"-> info.endDate.get.toJson)
      map += ("attemptIndex" -> JsNumber(info.attemptIndex))
      val tasksDependedOn = info.task.tasksDependedOn.map(task => task.taskInfo.taskId).toIterable
      map += ("tasksDependedOn" -> tasksDependedOn.toJson)
      val tasksDependingOnThisTask = info.task.tasksDependingOnThisTask.map(task => task.taskInfo.taskId).toIterable
      map += ("tasksDependingOnThisTask" -> tasksDependingOnThisTask.toJson)
      JsObject(map)
    }

    override def read(json: JsValue): TaskExecutionInfo = {
      val jsObject = json.asJsObject

      val name = jsObject.fields("name").convertTo[String]
      val taskId = jsObject.fields("taskId").convertTo[TaskId]
      val status = jsObject.fields("status").convertTo[TaskStatus.Value]
      val script = jsObject.fields("script").convertTo[Path]
      val logFile = jsObject.fields("logFile").convertTo[Path]
      val submissionDate = instantOrNone(jsObject, "submissionDate")
      val resources = jsObject.fields("resources").convertTo[ResourceSet]
      val startDate= instantOrNone(jsObject, "startDate")
      val endDate = instantOrNone(jsObject, "endDate")
      val attemptIndex = jsObject.fields("attemptIndex").convertTo[Int]

      val task = taskManager.taskFor(taskId) match {
        case Some(t) => t
        case None => throw new IllegalArgumentException(s"Could not retrieve task with id '$taskId'")
      }

      val info = new TaskExecutionInfo(
        task           = task,
        taskId         = taskId,
        status         = status,
        script         = script,
        logFile        = logFile,
        submissionDate = submissionDate,
        resources      = resources,
        startDate      = startDate,
        endDate        = endDate,
        attemptIndex   = attemptIndex
      )
      if (info.task.name.compareTo(name) != 0) throw new IllegalStateException(s"Task names differ! '$name' != '${info.task.name}'")

      info
    }
  }

  private def stringOrNone(json: JsObject, key: String): Option[String] = {
    if (json.getFields(key).nonEmpty) json.fields(key).convertTo[Option[String]] else None
  }

  private def instantOrNone(json: JsObject, key: String): Option[Instant] = {
    if (json.getFields(key).nonEmpty) json.fields(key).convertTo[Option[Instant]] else None
  }
}