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

import dagr.api.models.util.Cores
import dagr.api.models.tasksystem.TaskStatus
import dagr.api.models.util.{Cores, Memory, ResourceSet, TimePoint}
import upickle.Js

/** Methods for pickling objects that will be sent or received via JSON. */
object DagrJsConversions {

  /** The value when an [[Option]] is [[None]]. */
  private val MissingValue: String = ""

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
      ("failure", Js.Str(status.failure.toString)),
      ("executing", Js.Str(status.executing.toString)),
      ("description", Js.Str(status.description))
    )
  }

  def toTaskStatus(value: Js.Value): TaskStatus = value match {
    case obj: Js.Obj =>
      new TaskStatus {
        override val name: String        = obj("name").str
        override val ordinal: Int        = obj("ordinal").num.toInt
        override val success: Boolean    = obj("success").str.toBoolean
        override val failure: Boolean    = obj("failure").str.toBoolean
        override val executing: Boolean  = obj("executing").str.toBoolean
        override val description: String = obj("description").str
      }
    case _ => throw upickle.Invalid.Data(value, s"Expected Js.Obj")
  }

  def toInstant(value: Js.Value): Instant = value match {
    case obj: Js.Str =>
      obj.str.split(',').toList match {
        case seconds :: nano :: Nil =>
          Instant.ofEpochSecond(seconds.toLong, nano.toInt)
        case _ =>
         throw upickle.Invalid.Data(obj, s"Could not parse instant '${obj("instant").str}'")
      }
    case _ =>
      throw upickle.Invalid.Data(value, s"Expected Js.Str")
  }

  def fromInstant(instant: Instant): Js.Value = {
    val seconds = instant.getEpochSecond
    val nano    = instant.getNano
    Js.Str(s"$seconds,$nano")
  }

  def fromTimePoint(timePoint: TimePoint): Js.Value = {
    Js.Obj(
      ("status", fromTaskStatus(timePoint.status)),
      ("instant", fromInstant(timePoint.instant))
    )
  }

  def toTimePoint(value: Js.Value): TimePoint = value match {
    case obj: Js.Obj =>
      TimePoint(
        status  = toTaskStatus(obj("status")),
        instant = toInstant(obj("instant"))
      )
    case _ => throw upickle.Invalid.Data(value, s"Expected Js.Obj")
  }
}
