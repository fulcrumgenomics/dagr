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

package dagr.ui

import autowire._
import dagr.api.models.{TaskInfo, TaskInfoResponse, TaskStatus}
import dagr.api.{DagrApi, DagrApiConfiguration}
import org.querki.jsext.{JSOptionBuilder, OptMap, noOpts}
import org.scalajs.dom
import org.scalajs.dom.html.Table
import org.scalajs.dom.raw.{Element, HTMLElement}
import org.scalajs.jquery.JQuery
import upickle.Js
import upickle.Js.Value
import upickle.default.{Reader, Writer, readJs, writeJs}

import scala.concurrent.{Await, Future}
import scala.scalajs.concurrent.JSExecutionContext.Implicits.queue
import scala.scalajs.js.annotation.{JSExport, JSExportTopLevel, JSGlobalScope}
import scalatags.JsDom.all._
import scala.scalajs.js
import org.scalajs.jquery.jQuery

import scala.collection.mutable
import js.Dynamic.{global => g}
import scala.concurrent.duration.Duration

object Client extends autowire.Client[Js.Value, Reader, Writer] with DagrApiConfiguration {
  override def doCall(req: Request): Future[Js.Value] = {
    val path = Seq("http://0.0.0.0:8080", root, version) ++ req.path.drop(DagrApi.prefixSegments.length)
    dom.ext.Ajax.post(
      url  = path.mkString("/"),
      data = upickle.json.write(Js.Obj(req.args.toSeq:_*))
    ).map(_.responseText).map(upickle.json.read(_))
  }

  def read[Result: Reader](p: Js.Value): Result = readJs[Result](p)
  def write[Result: Writer](r: Result): Value = writeJs(r)
}

/*
@js.native
trait DataTableElement extends dom.Element {
  def DataTable(): Unit = js.native
}

object DataTableElement {
  implicit def element2dataTableElement(e: dom.Element): DataTableElement = e.asInstanceOf[DataTableElement]
}
*/

@JSExportTopLevel("DagrUi")
object DagrUi extends js.JSApp {
  import JQueryDataTableFacade._

  type DataTable = JQuery

  private def escapeHtml(str: String): String = {
    val regex = "[\"'&<>]"
    if (str.matches(regex)) {
      return str
    }

    str.map {
      case 34 => "&quot;" // "
      case 38 => "&amp;"  // &
      case 39 => "&#39;"  // '
      case 60 => "&lt;"   // <
      case 62 => "&gt"    // >
      case c  => "" + c
    }.mkString
  }

  // TODO: escape angle brackets
  private def formatFileContents(path: Option[String],
                                 contents: Option[String],
                                 tailLines: Int = 5): String = path.map { p =>
    contents.map { c =>
      s"$p:\n" + c.split('\n').takeRight(tailLines).mkString("\n")
    }.getOrElse("No contents")
  }.getOrElse("Path is undefined")

  def main(): Unit = {
    main(Array.empty)
  }

  /*
  @JSExport
  def format(data: js.Array[js.Object]): Unit = {
    val id = data(2).valueOf().toString.toInt

  }
  */

  //var taskDataTable: JQueryDataTable = _
  def taskDataTable = g.taskDataTable

  // TODO: make each entry a case class
  val TaskTableNames = Seq("", "Name", "Id", "Status", "Status Description", "Attempts", "Last Update", "Script", "Log", "Depends On", "Dependents", "Parent", "Children")
  val TaskTableIds   = Seq("", "name", "id", "status", "description", "attempts", "last", "script", "log", "depends_on", "dependents", "parent", "children")
  val ToHide = Set("Script", "Log", "Depends On", "Dependents", "Parent", "Children")


  def taskNamesAndIdsToHTMLElement(namesAndIds: Seq[(String, BigInt)]): HTMLElement = {
    table(
      cls:=TableClasses,
      thead(
        tr(
          th("Name", width:="50%"),
          th("Id", width:="50%")
        )
      ),
      tbody(
        namesAndIds.map { case ((n: String, i: BigInt)) =>
          tr(
            td(n),
            td(i.toString)
          )
        }
      )
    ).render
  }

  private val TableClasses: String = "display table table-striped table-bordered table-hover"

  private def toTaskStatusClass(status: TaskStatus): String = {
    if (status.success) "success"
    else if (status.executing) "info"
    else if (status.failure) "danger"
    else ""
  }

  private def updateProgressBar(infos: Seq[TaskInfo]): Unit = {
    val length    = if (infos.isEmpty) Double.MaxValue else infos.length
    val successes = 100 * infos.count(_.status.success) / length.toDouble
    val executing = 100 * infos.count(_.status.executing) / length.toDouble
    val failures  = 100 * infos.count(_.status.failure) / length.toDouble
    val isActive  = successes + failures == 100.0

    def update(progressBar: Element, value: Double, message: String): Unit = {
      val valueStr = f"$value%.1f"
      progressBar.innerHTML = s"$valueStr% $message"
      progressBar.setAttribute("aria-valuenow", valueStr)
      progressBar.setAttribute("style", s"width: $valueStr%;")
      if (isActive && progressBar.classList.contains("active")) {
        progressBar.classList.remove("active")
        progressBar.classList.remove("progress-bar-striped")

      }
      progressBar.render
    }

    update(progressBarSuccesses, successes, "Succeeded")
    update(progressBarExecuting, executing, "Executing")
    update(progressBarFailures, failures, "Failed")
    update(progressBarOther, 100 - successes - executing - failures, "Pending")
  }

  private def toProgressBar(className: String): Element = {
    div(
      cls:=s"$className progress-bar progress-bar-striped active",
      attr("role"):="progress-bar",
      attr("aria-valuenow"):="0",
      attr("aria-valuemin"):="0",
      attr("aria-valuemax"):="100",
      color:="black !important",
      width:="0%",
      "0% Complete"
    ).render
  }

  private val progressBarSuccesses = toProgressBar("progress-bar-success")
  private val progressBarExecuting = toProgressBar("progress-bar-info")
  private val progressBarFailures  = toProgressBar("progress-bar-danger")
  private val progressBarOther     = toProgressBar("progress-bar-warning")

  @JSExport
  def main(args: Array[String]): Unit = {

    val topBox = div(
      h2(
        "Dagr Dashboard"
      )
    ).render

    // The task progress bar
    val progressBar = div(
      cls:="progress panel",
      progressBarSuccesses,
      progressBarExecuting,
      progressBarFailures,
      progressBarOther
    ).render

    // The task summary box
    val taskSummaryBox = div.render

    // The task table box
    val taskTableBox = div.render

    // The execution report box
    val executionReportBox = div.render

    // Initialize the table
    val taskTableBody = tbody().render
    val taskTable: Table = table(
      id := "tasks",
      cls := TableClasses,
      width := "100%",
      thead(
        tr(
          TaskTableNames.map { n =>
            if (ToHide.contains(n)) th(cls:="hidden", if (n.isEmpty) null else n)
            else th(n)
          }
        )
      ),
      taskTableBody
    ).render
    taskTableBox.appendChild(taskTable).render

    def updateOutput() = {
      Client[DagrApi].query(script = true, log = true, report = true).call().foreach { response: TaskInfoResponse =>

        // Update the progress bar
        updateProgressBar(response.infos)

        /*/ Get a map from id to name for all returned tasks. */
        val idToName = response.infos.flatMap { info =>
          info.id.map { i => (i, info.name)}
        }.toMap

        /** Formats the list of ids in an html table if present, otherwise gives the missing message in a paragraph. */
        def toNames(ids: Seq[BigInt], missingMsg: String): HTMLElement = {
          if (ids.isEmpty) p(missingMsg).render
          else {
            val namesAndIds = ids.map { id =>
              val name = idToName.getOrElse(id, s"Unknown name") + s" ($id)"
              (name, id)
            }
            taskNamesAndIdsToHTMLElement(namesAndIds)
          }
        }

        // Update the task summary
        {
          // TODO: get all the statuses via an dedicate end point
          taskSummaryBox.innerHTML = ""
          taskSummaryBox.appendChild(
            table(
              id := "taskSummary",
              cls := TableClasses,
              thead(
                tr(
                  th("Status"),
                  th("# of Tasks"),
                  th("Description")
                )
              ),
              tbody(
                response.infos
                  .map(_.status)
                  .groupBy { s => s.ordinal}
                  .toSeq
                  .sortBy(_._1)
                  .map { case ((_, statuses)) =>
                    val status = statuses.head
                      tr(
                        td(cls:=toTaskStatusClass(status),status.name),
                        td(statuses.length.toString),
                        td(status.description)
                      )
                  }
              )
            ).render
          ).render
        }

        // Update the task table
        {
          taskTableBody.innerHTML = ""
          taskTableBody.appendChild(
            response.infos.map { info =>
              tr(
                Seq(
                  td(cls:="details-control"),
                  td(info.name),
                  td(info.id.getOrElse(-1).toString),
                  td(
                    cls:=toTaskStatusClass(info.status),
                    info.status.name
                  ),
                  td(info.status.description),
                  td(info.attempts),
                  td(info.statusTime.toString),
                  td(cls:="hidden", pre(code(formatFileContents(info.script, info.scriptContents))).render),
                  td(cls:="hidden", pre(code(formatFileContents(info.log, info.logContents))).render),
                  td(cls:="hidden", toNames(info.dependsOn, missingMsg="Did not depend on any tasks")),
                  td(cls:="hidden", toNames(info.dependents, missingMsg="No tasks depended on this task")),
                  td(cls:="hidden", toNames(info.parent.map(i => Seq(i)).getOrElse(Seq.empty), missingMsg="No parent")),
                  td(cls:="hidden", toNames(info.children, missingMsg="No children"))
                )
              )
            }.render
          ).render

          // table with basic info
          taskTableBox.innerHTML = ""
          taskTableBox.appendChild {
            taskTable.render
          }.render

          taskTableBox.render
          // FIXME: not getting called
          if (g.taskDataTable != null) {
            dom.document.getElementById("tasks").asInstanceOf[js.Dynamic].DataTable().draw(false)
            //g.taskDataTable.rows().invalidate().draw()
          }
        }

        // Update the report
        {
          val report: String = response.report.getOrElse("No Report")
          executionReportBox.innerHTML = ""
          executionReportBox.appendChild(
            p(
              pre(
                code(
                  report
                )
              )
            ).render
          )
        }
      }
    }

    updateOutput()
    /*
    js.timers.setInterval(1000) {
      updateOutput()
    }
    */

    val tabbedMenuBox = div(
      id:="tabbed-menu",
      cls:="panel",
      ul(
        cls:="nav nav-tabs",
        li(
          cls:="active",
          a(
            attr("data-toggle"):="tab",
            href:="#tabbed-menu-1",
            "Task Summary"
          )
        ),li(
          a(
            attr("data-toggle"):="tab",
            href:="#tabbed-menu-2",
            "Task Table"
          )
        ),
        li(
          a(
            attr("data-toggle"):="tab",
            href:="#tabbed-menu-3",
            "Execution Log"
          )
        )
        ,
        li(
          a(
            attr("data-toggle"):="tab",
            href:="#tabbed-menu-4",
            "Settings"
          )
        )
      ),
      div(
        cls:="tab-content",
        div(
          id:="tabbed-menu-1",
          cls:="tab-pane fade in active",
          taskSummaryBox
        ),div(
          id:="tabbed-menu-2",
          cls:="tab-pane fade",
          taskTableBox
        ),
        div(
          id:="tabbed-menu-3",
          cls:="tab-pane fade",
          executionReportBox
        )
        ,
        div(
          id:="tabbed-menu-4",
          cls:="tab-pane fade",
          ul(
            li("Task summary table - toggle missing statuses")
          )
        )
      )
    ).render

    dom.document.body.appendChild(
      header(
        cls := "container panel",
        style:="text-align:center;padding-bottom: 20px;",
        topBox
      ).render
    )
    dom.document.body.appendChild(
      div(
        cls := "container",
        progressBar,
        tabbedMenuBox
      ).render
    )
    dom.document.body.appendChild(
      footer(
        cls := "container panel",
        p(
          style:="text-align:center;padding-top: 20px;",
          a(
            href:="https://github.com/fulcrumgenomics/dagr",
            "https://github.com/fulcrumgenomics/dagr"
          )
        )
      ).render
    )

    jQuery(() =>
      g.initOrUpdateTaskDataTable()
    )

    // FIXME: https://github.com/jducoeur/jsext/issues/13
    /*
    jQuery(() =>
      // Construct the DataTable for the tasks
      taskDataTable = {
        import JQueryDataTableFacade._
        //val controlColumn    = DataTableColumnOptions.data(null).orderable(false).className("details-control").defaultContent("")
        //val dataTableColumns = controlColumn +: taskTableIds.drop(1).map { _ => DataTableColumnOptions.data(null) }
        val dataTableColumns = taskTableIds.map { id => DataTableColumnOptions.data(id) }
        val dataTableOptions = DataTableOptions.destroy(true).columns(js.Array(dataTableColumns:_*)) //DataTableOptions.columns(js.Array(dataTableColumns:_*)).destroy(true)
        jQuery("#tasks").DataTable(dataTableOptions)
      }
    )
    */
  }
}

@js.native
trait JQueryDataTable extends js.Object {
  def rows(): JQueryDataTableRow = js.native
  def draw(): JQueryDataTableRow = js.native
}

@js.native
trait JQueryDataTableRow extends js.Object {
  def invalidate(): JQueryDataTable = js.native
}

@js.native
trait JQueryDataTableFacade extends js.Object {
  def DataTable(options: DataTableOptions): JQueryDataTable = js.native
}

object JQueryDataTableFacade {
  implicit def jq2DataTable(jq: JQuery): JQueryDataTableFacade = jq.asInstanceOf[JQueryDataTableFacade]
  implicit def element2dataTableElement(e: dom.Element): JQueryDataTableFacade = e.asInstanceOf[JQueryDataTableFacade]
}


@js.native
trait DataTableOptions extends js.Object
object DataTableOptions extends DataTableOptionsBuilder(noOpts)
class DataTableOptionsBuilder(val dict: OptMap)
  extends JSOptionBuilder[DataTableOptions, DataTableOptionsBuilder](new DataTableOptionsBuilder(_)) {
  def columns(options: js.Array[DataTableColumnOptionsBuilder]) = jsOpt("columns", options)
  def destroy(b: Boolean) = jsOpt("destroy", b)
}

@js.native
trait DataTableColumnOptions extends js.Object
object DataTableColumnOptions extends DataTableColumnOptionsBuilder(noOpts)
class DataTableColumnOptionsBuilder(val dict: OptMap)
extends JSOptionBuilder[DataTableColumnOptions, DataTableColumnOptionsBuilder](new DataTableColumnOptionsBuilder(_)) {
  def data(v: String)           = jsOpt("data", v)
  def className(v: String)      = jsOpt("className", v)
  def orderable(v: Boolean)     = jsOpt("orderable", v)
  def defaultContent(v: String) = jsOpt("defaultContent", v)
}