/*
 * The MIT License
 *
 * Copyright (c) 2017 LLC Fulcrum Genomics
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

package dagr.core.reporting

import com.fulcrumgenomics.commons.CommonsDef.{FilePath, SafelyClosable}
import com.fulcrumgenomics.commons.io.Io
import dagr.api.models.tasksystem.TaskStatus
import dagr.core.reporting.ReplayLogger.{Definition, Status}
import dagr.core.reporting.ReportingDef._
import dagr.core.tasksystem.Task
import dagr.core.tasksystem.Task.TaskInfo

import scala.collection.mutable

object ReplayLogger {

  val Separator: String = ","

  trait ToStringIsSimpleNameUpperCase extends Product {
    override def toString: String = {
      val name = this.getClass.getSimpleName.replaceFirst("[$].*$", "").toUpperCase
      (Seq(name) ++ this.productIterator.map(_.toString)).mkString(Separator)
    }
  }

  object Definition {
    val Name: String        = "DEFINITION"
    val Header: Seq[String] = Seq("SIMPLE_NAME", "NAME", "CODE", "PARENT_CODE", "CHILD_NUMBER")

    /** Removes various characters from the simple class name, for scala class names. */
    def getSimpleName(task: Task): String = {
      task.getClass.getSimpleName.replaceFirst("[$].*$", "")
    }

    def buildRootDefinition(root: Task): Definition = {
      Definition(root, -1, 0)
    }

    def isRootDefinition(definition: Definition): Boolean = {
      definition.parentCode == -1 && definition.childNumber == 0
    }

    def apply(line: String): Definition = {
      line.split(Separator).toList match {
        case Name :: simpleName :: name  :: code :: parentCode :: childNumber :: Nil =>
          Definition(simpleName, name, code.toInt, parentCode.toInt, childNumber.toInt)
        case _ =>
          throw new IllegalArgumentException(s"Could not parse line: $line")
      }
    }
    def apply(task: Task, parentCode: Int, childNumber: Int): Definition = {
      Definition(getSimpleName(task), task.name, task.hashCode, parentCode, childNumber)
    }
  }

  /** Attempts to uniquely identify a task.
    * @param simpleName the simple class name for the task
    * @param name the actual name of the task (not considered, for debugging only)
    * @param code a unique number for this definition
    * @param parentCode a unique number fo the parent's definition
    * @param childNumber the 0-based index of the child relative to all children of the parent
    */
  case class Definition(simpleName: String, name: String, code: Int, parentCode: Int, childNumber: Int) extends ToStringIsSimpleNameUpperCase {
    /** Returns true if the the other definition represents the same task as the this definition.  Assumes that
      * the parents have the same number of child tasks. */
    def equivalent(that: Definition): Boolean = {
      this.simpleName == that.simpleName && this.parentCode == that.parentCode && this.childNumber == that.childNumber
    }
  }

  object Status {
    val Name: String        = "STATUS"
    val Header: Seq[String] = Seq("TASK_CODE", "STATUS_NAME", "STATUS_ORDINAL")

    def apply(line: String): Status = {
      line.split(Separator).toList match {
        case Name :: taskCode :: statusName :: statusOrdinal :: Nil =>
          Status(taskCode.toInt, statusName, statusOrdinal.toInt)
        case _ =>
          throw new IllegalArgumentException(s"Could not parse line: $line")
      }
    }
    def apply(status: TaskStatus, taskDefinition: Definition): Status = {
      Status(taskDefinition.code, status.name, status.ordinal)
    }
  }

  /** Stores the status of a task at a given point in time. */
  case class Status(definitionCode: Int, statusName: String, statusOrdinal: Int) extends ToStringIsSimpleNameUpperCase
}

/** A replay logger logs the definitions of a task (when they are built) and when their statuses are updated to enable
  * execution to be replayed.  The replay produced can be used by a [[dagr.core.exec.TaskCache]] in a subsequent execution.
  *
  * The resulting log will contain a header (lines starting with #) and a line per task definition and status change
  * respectively.  New tasks are logged via a [[Definition]] line, which uniquely identifies the task relative to its
  * parent.  Status changes are logged via a [[Status]] line, which identifies the task and updated status.
  * */
class ReplayLogger(log: FilePath) extends TaskLogger with TaskRegister with AutoCloseable {
  private var registeredRootTask: Boolean = false
  private var closed: Boolean = false
  private val writer = Io.toWriter(log)
  private val taskToDefinition: mutable.Map[Task, Definition] = new mutable.HashMap[Task, Definition]()

  // Close in case of something unexpected
  sys.addShutdownHook({
    this.close()
  })

  writeHeader()

  /** The method that will be called on the result of `Task.getTasks`. */
  def register(parent: Task, child: Task*): Unit = {
    if (child.forall(_ == parent)) {
      if (!registeredRootTask) {
        val rootDefinition = Definition.buildRootDefinition(parent)
        logTaskDefinition(parent, rootDefinition.parentCode, rootDefinition.childNumber)
        registeredRootTask = true
      }
    }
    else {
      val parentDefinition = this.taskToDefinition(parent)
      child.zipWithIndex.foreach { case (_child, childNumber) =>
        // log the definition of each child task
        logTaskDefinition(_child, parentDefinition.code, childNumber)
      }
    }
  }

  /** The method that will be called with updated task information. */
  def record(info: TaskInfo): Unit = logStatusChange(info)

  /** Closes the underlying logging file. */
  def close(): Unit = if (!this.closed) {
    this.writer.flush()
    this.writer.safelyClose()
    this.closed = true
  }

  /** Writes the given string and flushes the writer. */
  private def write(str: String): Unit = this.synchronized {
    this.writer.write(str)
    this.writer.flush()
  }

  /** Writes some debug information to the top of the log. */
  private def writeHeader(): Unit = {
    write((Seq(s"#${Definition.Name}") ++ Definition.Header).mkString(",") + "\n")
    write((Seq(s"#${Status.Name}") ++ Status.Header).mkString(",") + "\n")
  }

  /** Logs the definition of the task. */
  private def logTaskDefinition(task: Task, parentCode: Int, childNumber: Int): Unit = {
    val definition = Definition(task, parentCode, childNumber)
    this.taskToDefinition(task) = definition
    write(definition.toString + "\n")
  }

  /** Logs the a status update for the task. */
  private def logStatusChange(rootInfo: TaskInfo): Unit = {
    val definition = this.taskToDefinition(rootInfo.task)
    write(Status(rootInfo.status, definition).toString + "\n")
  }
}
