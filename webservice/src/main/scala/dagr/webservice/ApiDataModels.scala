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

import dagr.core.tasksystem.Task.TaskInfoLike
import dagr.tasks.DagrDef.FilePath

/** Stores the data to be returned by an end-point. Make sure that there exists a protocol and any custom JSON
  * handling specified in [[DagrApiJsonSupport]].
  */
sealed abstract class DagrResponse

case class DagrVersionResponse(id: String) extends DagrResponse

case class DagrStatusResponse(infos: Iterable[TaskInfoLike]) extends DagrResponse

case class DagrTaskScriptResponse(script: Option[FilePath]) extends DagrResponse

case class DagrTaskLogResponse(log: Option[FilePath]) extends DagrResponse

case class DagrTaskInfoResponse(info: TaskInfoLike) extends DagrResponse
