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

package dagr.api

import java.time.Instant

import dagr.api.DagrApi.TaskId
import dagr.api.DagrStatusApi.StatusResponse
import dagr.api.models._
import dagr.api.models.tasksystem.{TaskInfo, TaskStatus}
import dagr.api.models.util.{ResourceSet, TimePoint}
import upickle.Js
import upickle.default.{Reader, Writer}

import scala.collection.mutable.ListBuffer

/** Use this trait to access methods to query the status of pipelines and tasks in Dagr. */
trait DagrStatusApi {
  // Developer note: this trait cannot extend anything, as required by autowire

  /**
    * Queries Dagr for tasks it knows about.  The parameters can be used to filter or select specific tasks.  Parameters
    * are ignored if empty.
    *
    * @param name A regular expression to match on the task name (see the [[java.util.regex]] package of the Java Platform).
    * @param status The status(es) of the tasks.
    * @param id The set of task identifiers.
    * @param attempts The set of number of attempts.
    * @param minResources The minimum resources with which the task was scheduled.
    * @param maxResources The maximum resources with which the task was scheduled.
    * @param since Require time the last status was reached to be at least this time.
    * @param until Require time the last status was reached to be at most this time.
    * @param dependsOn The task(s) returned should (each) depend on at least one of these tasks.
    * @param dependents The task(s) returned should (each) have one of these tasks depend on it.
    * @param parent The parent of the task.
    * @param children The task(s) returned should (each) have at least one of these children.
    * @param script True to return the contents of the scripts file, false otherwise.
    * @param log True to return the contents of the log file, false otherwise.
    * @param report True to return the contents of the report file, false otherwise.
    * @return
    */
  def status(name: Seq[String] = Seq.empty,
             status: Seq[String] = Seq.empty,
             id: Seq[TaskId] = Seq.empty,
             attempts: Seq[Int] = Seq.empty,
             minResources: Option[ResourceSet] = None,
             maxResources: Option[ResourceSet] = None,
             since: Option[String] = None,
             until: Option[String] = None,
             dependsOn: Seq[TaskId] = Seq.empty,
             dependents: Seq[TaskId] = Seq.empty,
             parent: Option[TaskId] = None,
             children: Seq[TaskId] = Seq.empty,
             script: Boolean = false,
             log: Boolean = false,
             report: Boolean = false
            ): StatusResponse

  /** Returns the contents of the log file for the task with the given identifier. */
  def log(id: TaskId): String

  /** Returns the contents of the script file for the task with the given identifier. */
  def script(id: TaskId): String

  /** Returns the contents of the report file for the current execution. */
  def report(id: TaskId): String = "Not yet implemented"

  /** Returns the list of statuses, ordered by ordinal determined by the underlying execution system. */
  def statuses(): Seq[TaskStatus]
}

object DagrStatusApi {

  /** The fully-qualified path to the DagrApi trait.  The fully-qualified path is
    * required for scala-js and autowire. */
  val FullyQualifiedPath: List[String] = List("dagr", "api", "DagrStatusApi")

  /** The response returned from the [[dagr.api.DagrStatusApi.status()]] method, providing the task information for reach
    * task found, optionally with the execution report. */
  case class StatusResponse(infos: Seq[TaskStatusResponse], report: Option[String] = None)

  object StatusResponse {

    implicit class InfoId(info: TaskStatusResponse) {
      def idStr: String = info.id.map(_.toString).getOrElse("None")
    }

    /** Writer for writing a [[dagr.api.DagrStatusApi.StatusResponse]] to JSON. */
    implicit val statusResponseToWriter: Writer[StatusResponse] = Writer[StatusResponse] {
      response: StatusResponse =>
        val tuples = response.infos.map { info =>
          (info.idStr, TaskStatusResponse.taskStatusResponseToWriter.write(info))
        }
        val infos = Js.Obj(tuples:_*)
        response.report match {
          case Some(report) => Js.Obj(("infos", infos), ("report", Js.Str(report)))
          case None         => Js.Obj(("infos", infos))
        }
    }

    /** Reader for reading a [[dagr.api.DagrStatusApi.StatusResponse]] from JSON. */
    implicit val statusResponseToReader: Reader[StatusResponse] = Reader[StatusResponse] {
      case obj: Js.Obj =>

        val infos = obj("infos").obj.flatMap { case (id, value) =>
          value match {
            case o: Js.Obj =>
              val info = TaskStatusResponse.taskStatusResponseToReader.read(o)
              //if (info.idStr != id) throw upickle.Invalid.Data(value, s"Task ids did not match ('$id' != '${info.idStr}'")
              Some(info)
            case x =>
              None
          }
        }.toSeq
        val report = if (obj.value.exists(_._1 == "report")) Some(obj("report").str) else None
        StatusResponse(infos=infos, report=report)
    }
  }

  /** Stores the state of execution for a single task returned by the [[dagr.api.DagrStatusApi.status()]] method.
    *
    * @param name The name of the task.
    * @param id The task identifier.
    * @param attempts The number of attempts at executing.
    * @param script The path to the script (if any).
    * @param log The path to the log (if any).
    * @param resources The amount of resources used by this task (if scheduled).
    * @param exitCode The exit code (if completed).
    * @param throwable The throwable of the task (if failed with an exception).
    * @param status The status of the task.
    * @param timePoints The time points of task (each time point at which the status was changed).
    * @param statusTime The time at which the current status was changed.
    * @param scriptContents The contents of the script file, if present.
    * @param logContents The contents of the log file, if present
    * @param dependsOn The tasks on which this task depends.
    * @param dependents The tasks that depend on this task.
    * @param parent The task that created this task.
    * @param children The tasks that this task created.
    */
  case class TaskStatusResponse
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
    override val statusTime     : Instant,
    // Optional fields, if empty, no meaning should be inferred
    scriptContents : Option[String] = None,
    logContents    : Option[String] = None,
    dependsOn      : Seq[TaskId]  = Seq.empty,
    dependents     : Seq[TaskId] = Seq.empty,
    parent         : Option[TaskId]  = None,
    children       : Seq[TaskId]   = Seq.empty
  ) extends TaskInfo[String] with Ordered[TaskStatusResponse] {

    /** Compares two tasks by their status ordinal. */
    def compare(that: TaskStatusResponse): Int = {
      (this.id, that.id) match {
        case (Some(_), None)    => -1
        case (None, Some(_))    => 1
        case (None, None)       => this.status.ordinal - that.status.ordinal
        case (Some(l), Some(r)) => (l - r).toInt
      }
    }
  }

  object TaskStatusResponse {
    import DagrJsConversions._

    /** Writer for writing the task info to JSON. */
    implicit val taskStatusResponseToWriter: Writer[TaskStatusResponse] = Writer[TaskStatusResponse] {
      info: TaskStatusResponse =>
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

    /** Reader from parsing the task info from JSON. */
    implicit val taskStatusResponseToReader: Reader[TaskStatusResponse] = Reader[TaskStatusResponse] {
      case obj: Js.Obj =>
        val names = obj.value.map(_._1).toSet

        def ifContains[T](name: String)(f: => T): Option[T] = if (names.contains(name)) Some(f) else None

        TaskStatusResponse(
          name           = obj("name").str,
          id             = ifContains("id") { obj("id").num.toInt },
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
          dependsOn      = obj("depends_on").arr.map(_.num.toInt),
          dependents     = obj("dependents").arr.map(_.num.toInt),
          parent         = ifContains("parent") { obj("parent").num.toInt },
          children       = obj("children").arr.map(_.num.toInt)
        )
    }
  }
}
