package dagr.ui

import dagr.core.tasksystem.Task.TaskInfoLike
import dagr.core.util.TaskInfo
import org.scalajs.dom
import org.scalajs.jquery.jQuery

import scala.scalajs.js
import scala.scalajs.js.{JSApp, JSON}
import scala.scalajs.js.annotation.JSExportTopLevel
import spray.json._

object TutorialApp extends JSApp {

  def main(): Unit = {
    main(Array.empty)
  }

  def main(args: Array[String]): Unit = {
    jQuery(() => setupUI())
  }

  @JSExportTopLevel("addClickedMessage")
  def addClickedMessage(): Unit = {
    jQuery("body").append("<p>You clicked the button!</p>")

    val xhr = new dom.XMLHttpRequest()
    xhr.open("GET", "http://0.0.0.0:8080/service/v1/info")
    xhr.withCredentials = false
    xhr.onload = { (e: dom.Event) =>
      if (xhr.status == 200) {
        //jQuery("body").append(s"<p>${xhr.responseText}</p>")

        val j = upickle.default.read[List[TaskInfo]](xhr.responseText)
        jQuery("body").append(s"<p>$j</p>")

        /*
        val json  = xhr.responseText.parseJson
        val infos = json.convertTo[List[String]]
        infos.foreach { str =>
          val info = TaskInfo.parse(str)
          jQuery("body").append(s"<p>$info</p>")
        }
        */

        val json =  JSON.parse(xhr.responseText)
        json.list match {
          case jsonlist: js.Array[js.Dynamic] =>
            for (j <- jsonlist) {
              jQuery("body").append(s"<p>HERE 1${j}</p>")
              jQuery("body").append(s"</br></br></br>")

            }
          case _ => jQuery("body").append(s"No Results: $json")
        }
      }
    }
    xhr.send()

    jQuery("#example").find("tbody").append("<tr><td>In the table</td><tr>")
  }

  def setupUI(): Unit = {
    jQuery("#click-me-button").click(() => addClickedMessage())
    jQuery("body").append("<p>Hello World</p>")
    //jQuery("#example").context.DataTable()
  }

  @js.native
  trait DataTableElement extends dom.Element {
    def DataTable(): Unit = js.native
  }

  object DataTableElement {
    implicit def element2dataTableElement(e: dom.Element): DataTableElement =
    {
      e.asInstanceOf[DataTableElement]
    }
  }
}
