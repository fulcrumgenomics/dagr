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

import dagr.api.DagrApi.TaskId
import dagr.api.models._

/*
object DagrApiConfiguration {
  object Keys {
    val ApiVersion = "dagr.api.version"
    val ApiRoot    = "dagr.api.root"
  }
}
*/

/*
trait DagrApiConfiguration extends Configuration {
  lazy val version: String = optionallyConfigure[String](DagrApiConfiguration.Keys.ApiVersion).getOrElse("v1")
  lazy val root: String    = optionallyConfigure[String](DagrApiConfiguration.Keys.ApiRoot).getOrElse("api")
}
*/

trait DagrApiConfiguration {
  lazy val version: String = "v1"
  lazy val root: String    = "api"
}

object DagrApi {
  type TaskId = BigInt

  // *** IMPORTANT *** this must be the fully-qualified path to the DagrApi trait!
  val prefixSegments: List[String] = List("dagr", "api", "DagrApi")
}

// NB: this trait cannot extend anything, as required by autowire
trait DagrApi {


  final def query(name: Seq[String] = Seq.empty, // any name pattern
                  status: Seq[String] = Seq.empty, // name
                  id: Seq[TaskId] = Seq.empty, // any task ids
                  attempts: Seq[Int] = Seq.empty, // any attempt #
                  minResources: Option[String] = None, // TODO: why cant this be of type ResourceSet
                  maxResources: Option[String] = None, // TODO: why cant this be of type ResourceSet
                  since: Option[String] = None, // TODO: why cant this be of type Instant
                  until: Option[String] = None, // TODO: why cant this be of type Instant
                  dependsOn: Seq[TaskId] = Seq.empty,
                  dependents: Seq[TaskId] = Seq.empty,
                  parent: Option[TaskId] = None, // TODO: include this in the response
                  children: Seq[TaskId] = Seq.empty,
                  script: Boolean = false,
                  log: Boolean = false,
                  report: Boolean = false
                 ): TaskInfoResponse = {

    def toResourceSet(str: String): ResourceSet = {
      str.split(",").toList match {
        case cores :: memory :: Nil => new ResourceSet(Cores(cores.toDouble), Memory(memory))
        case _ => throw upickle.Invalid.Json(str, s"Could not extract resource set '$str'")
      }
    }

    _query(
      name         = name,
      status       = status,
      id           = id,
      attempts     = attempts,
      minResources = minResources.map(toResourceSet),
      maxResources = maxResources.map(toResourceSet),
      since        = since,
      until        = until,
      dependsOn    = dependsOn,
      dependents   = dependents,
      parent       = parent,
      children     = children,
      script       = script,
      log          = log,
      report       = report
    )
  }

  def log(id: TaskId): String

  def script(id: TaskId): String

  def report(id: TaskId): String = "Not yet implemented"

  def statuses(): Seq[TaskStatus]

  /** The method a server implementation should implement. */
  protected def _query(name: Seq[String] = Seq.empty, // any name pattern
                       status: Seq[String] = Seq.empty, // name
                       id: Seq[TaskId] = Seq.empty, // any task ids
                       attempts: Seq[Int] = Seq.empty, // any attempt #
                       minResources: Option[ResourceSet] = None,
                       maxResources: Option[ResourceSet] = None,
                       since: Option[String] = None, // since using statusTime
                       until: Option[String] = None, // until using statusTime,
                       dependsOn: Seq[TaskId] = Seq.empty,
                       dependents: Seq[TaskId] = Seq.empty,
                       parent: Option[TaskId] = None, // TODO: include this in the response
                       children: Seq[TaskId] = Seq.empty,
                       script: Boolean = false,
                       log: Boolean = false,
                       report: Boolean = false): TaskInfoResponse
}
