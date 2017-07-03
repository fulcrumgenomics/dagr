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

import dagr.api.DagrApi.TaskId
import upickle.Js
import upickle.default.{Reader, Writer}

import scala.collection.mutable.ListBuffer
import java.time.Instant

/** The execution information for a task.  Used for extrenal read-only access to [[TaskInfo]]. Any execution system
  * should extend [[TaskInfo]] instead class to store their specific metadata. */
trait TaskInfoLike  {
  def id         : Option[TaskId]
  def attempts   : Int
  def script     : Option[String]
  def log        : Option[String]
  def resources  : Option[ResourceSet]
  def exitCode   : Option[Int]
  def throwable  : Option[Throwable]

  /** The current status of the task. */
  def status     : TaskStatus

  /** The instant the task reached a given status. */
  def timePoints :  Traversable[TimePoint]

  /** The instant the task reached the current status. */
  def statusTime : Instant

  /** The name of the task. */
  def name: String

  protected[dagr] def infoString: String = {
    val resourceMessage = this.resources match {
      case Some(r) => s"with ${r.cores} cores and ${r.memory} memory"
      case None    => ""
    }
    s"'$name' : ${this.status} on attempt #${this.attempts} $resourceMessage"
  }
}

case class TaskInfo
(
  name           : String,
  id             : Option[TaskId],
  attempts       : Int,
  script         : Option[String],
  log            : Option[String],
  resources      : Option[ResourceSet],
  exitCode       : Option[Int],
  throwable      : Option[Throwable],
  status         : TaskStatus,
  timePoints     : Traversable[TimePoint],
  statusTime     : Instant,
  // Optional fields, if empty, no meaning should be inferred
  scriptContents : Option[String] = None,
  logContents    : Option[String] = None,
  dependsOn      : Seq[TaskId]  = Seq.empty,
  dependents     : Seq[TaskId] = Seq.empty,
  parent         : Option[TaskId]  = None,
  children       : Seq[TaskId]   = Seq.empty
) extends TaskInfoLike {

  def compare(that: TaskInfo): Int = {
    (this.id, that.id) match {
      case (Some(_), None)    => -1
      case (None, Some(_))    => 1
      case (None, None)       => this.status.ordinal - that.status.ordinal
      case (Some(l), Some(r)) => (l - r).toInt
    }
  }
}

object TaskInfo {
  import DagrApiDef._

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
      tuples += (("status_time", fromInstant(info.statusTime)))
      info.scriptContents.foreach { s => tuples += (("script_contents", Js.Str(s))) }
      info.logContents.foreach { l => tuples += (("log_contents", Js.Str(l))) }
      tuples += (("depends_on", Js.Arr(info.dependsOn.map(s => Js.Num(s.toInt)).toSeq:_*)))
      tuples += (("dependents", Js.Arr(info.dependents.map(s => Js.Num(s.toInt)).toSeq:_*)))
      info.parent.foreach { p => tuples += (("parent", Js.Num(p.toInt))) }
      tuples += (("children", Js.Arr(info.children.map(s => Js.Num(s.toInt)).toSeq:_*)))

      Js.Obj(tuples:_*)
  }

  implicit val query2Reader: Reader[TaskInfo] = Reader[TaskInfo] {
    case obj: Js.Obj =>
      val names = obj.value.map(_._1).toSet

      def ifContains[T](name: String)(f: => T): Option[T] = if (names.contains(name)) Some(f) else None

      TaskInfo(
        name           = obj("name").str,
        id             = ifContains("id") { BigInt(obj("id").num.toInt) },
        attempts       = obj("attempts").num.toInt,
        script         = ifContains("script") { obj("script").str },
        log            = ifContains("log") { obj("log").str },
        resources      = ifContains("resources") { toResourceSet(obj("resources")).get },
        exitCode       = ifContains("exit_code") { obj("exit_code").num.toInt },
        throwable      = ifContains("throwable") { new Throwable(obj("throwable").str) },
        status         = toTaskStatus(obj("status")),
        timePoints     = obj("time_points").arr.map(toTimePoint),
        statusTime     = toInstant(obj("status_time")),
        scriptContents = ifContains("script_contents") { obj("script_contents").str },
        logContents    = ifContains("log_contents") { obj("log_contents").str },
        dependsOn      = obj("depends_on").arr.map(_.num.toInt).map(BigInt(_)),
        dependents     = obj("dependents").arr.map(_.num.toInt).map(BigInt(_)),
        parent         = ifContains("parent") { BigInt(obj("parent").num.toInt) },
        children       = obj("children").arr.map(_.num.toInt).map(BigInt(_))
      )
  }
}