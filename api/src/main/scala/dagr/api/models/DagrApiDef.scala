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

import dagr.core.exec.{Cores, Memory, ResourceSet}
import dagr.core.tasksystem.Task.{TaskStatus, TimePoint}
import upickle.Js

object DagrApiDef {

  val MissingValue: String = ""

  implicit class WithMissing(str: Option[Any]) {
    def withMissing: String = str.map(_.toString).getOrElse(MissingValue)
  }

  def toInstant(value: Js.Value): Option[Instant] = value match {
    case str: Js.Str => if (str.str == MissingValue) None else Some(Instant.parse(str.str))
    case _ => throw upickle.Invalid.Data(value, s"Expected Js.Obj")
  }

  def fromResourceSet(resources: Option[ResourceSet]): Js.Value = resources match {
    case None    => Js.Str(MissingValue)
    case Some(r) =>
      Js.Obj(
        ("cores", Js.Str(r.cores.toString)),
        ("memory", Js.Str(r.memory.prettyString))
      )
  }

  def toResourceSet(value: Js.Value): Option[ResourceSet] = value match {
    case _: Js.Obj =>
      val obj = value.obj
      Some(ResourceSet(
        Cores(obj("cores").str.toDouble),
        Memory(obj("memory").str)
      ))
    case str: Js.Str if str.str == MissingValue => None
    case _ => throw upickle.Invalid.Data(value, s"Expected Js.Obj, or Js.Str with value '$MissingValue'")
  }

  def fromTaskStatus(status: TaskStatus): Js.Value = {
    Js.Obj(
      ("name", Js.Str(status.name)),
      ("ordinal", Js.Num(status.ordinal)),
      ("success", Js.Str(status.success.toString)),
      ("description", Js.Str(status.description))
    )
  }

  def toTaskStatus(value: Js.Value): TaskStatus = value match {
    case _: Js.Obj =>
      val obj = value.obj
      new TaskStatus {
        override val name: String        = obj("name").str
        override val ordinal: Int        = obj("ordinal").num.toInt
        override val success: Boolean    = obj("success").str.toBoolean
        override val description: String = obj("description").str
      }
    case _ => throw upickle.Invalid.Data(value, s"Expected Js.Obj")
  }

  def fromTimePoint(timePoint: TimePoint): Js.Value = {
    Js.Obj(
      ("status", fromTaskStatus(timePoint.status)),
      ("instant", Js.Str(timePoint.instant.toString))
    )
  }

  def toTimePoint(value: Js.Value): TimePoint = value match {
    case _: Js.Obj =>
      val obj = value.obj
      TimePoint(
        status  = toTaskStatus(value),
        instant = Instant.parse(obj("instant").str)
      )
    case _ => throw upickle.Invalid.Data(value, s"Expected Js.Obj")
  }
}
