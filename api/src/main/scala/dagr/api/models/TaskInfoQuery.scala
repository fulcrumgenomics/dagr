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

import dagr.core.DagrDef.TaskId
import dagr.core.exec.{Cores, Memory, ResourceSet}
import upickle.Js
import upickle.default.{Reader, Writer}


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
)

object TaskInfoQuery {
  import DagrApiDef._

  implicit val query2Writer: Writer[TaskInfoQuery] = Writer[TaskInfoQuery] {
    query: TaskInfoQuery =>
      Js.Obj(
        ("name", Js.Arr(query.name.map(Js.Str):_*)),
        ("status", Js.Arr(query.status.map(Js.Str):_*)),
        ("id", Js.Arr(query.id.map(i => Js.Str(i.toString)):_*)),
        ("attempts", Js.Arr(query.attempts.map(i => Js.Str(i.toString)):_*)),
        ("min_resources", fromResourceSet(query.minResources)),
        ("max_resources", fromResourceSet(query.minResources)),
        ("until", Js.Str(query.until.map(_.toString).withMissing)),
        ("since", Js.Str(query.since.map(_.toString).withMissing)),
        ("depends_on", Js.Arr(query.dependsOn.map(i => Js.Str(i.toString)):_*)),
        ("dependents", Js.Arr(query.dependents.map(i => Js.Str(i.toString)):_*)),
        ("parent", Js.Str(query.parent.map(_.toString).withMissing)),
        ("children", Js.Arr(query.children.map(i => Js.Str(i.toString)):_*))
      )
  }

  implicit val query2Reader: Reader[TaskInfoQuery] = Reader[TaskInfoQuery] {
    case obj: Js.Obj =>

      val parent = {
        val p = obj("parent").str
        if (p == MissingValue) None else Some(BigInt(p))
      }

      TaskInfoQuery(
        name         = obj("name").arr.map(_.str),
        status       = obj("status").arr.map(_.str),
        id           = obj("id").arr.map(i => BigInt(i.str)),
        attempts     = obj("attempts").arr.map(_.str.toInt),
        minResources = toResourceSet(obj("min_resources")),
        maxResources = toResourceSet(obj("max_resources")),
        until        = toInstant(obj("until")),
        since        = toInstant(obj("since")),
        dependsOn    = obj("depends_on").arr.map(s => BigInt(s.str)),
        dependents   = obj("dependents").arr.map(s => BigInt(s.str)),
        parent       = parent,
        children     = obj("children").arr.map(s => BigInt(s.str))
      )
  }
}
