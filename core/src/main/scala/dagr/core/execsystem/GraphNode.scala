/*
 * The MIT License
 *
 * Copyright (c) 2015-6 Fulcrum Genomics LLC
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
package dagr.core.execsystem

import dagr.core.DagrDef._
import dagr.core.execsystem.TaskManagerLike.BaseGraphNode
import dagr.core.tasksystem.Task

import scala.collection.mutable.ListBuffer

/** A node in the execution graph
  *
  * @param task             the task itself
  * @param predecessorNodes the current list of nodes on which this node depends
  * @param state            the execution state of this node
  * @param enclosingNode    Some other graph node that generated this node, and now depends on this node, or None.  This
  *                         is used to track submission, start, and end dates for the enclsoing node.  Typically enclosing
  *                         nodes are [[dagr.core.tasksystem.Pipeline]]s.
  */
class GraphNode(var task: Task,
                predecessorNodes: Traversable[GraphNode] = Nil,
                var state: GraphNodeState.Value = GraphNodeState.PREDECESSORS_AND_UNEXPANDED,
                val enclosingNode: Option[GraphNode] = None) extends BaseGraphNode {

  private val _predecessors = new ListBuffer[GraphNode]()

  _predecessors ++= predecessorNodes

  def taskId: TaskId = taskInfo.taskId
  def taskInfo: TaskExecutionInfo = task.execsystemTaskInfo

  /** Remove a predecessor from the execution graph.
   *
   * @param predecessor the predecessor to remove.
   * @return true if the predecessor was found and removed, false otherwise
   */
  def removePredecessor(predecessor: GraphNode): Boolean = {
    if (_predecessors.isEmpty || !_predecessors.contains(predecessor)) {
      false
    }
    else {
      _predecessors -= predecessor
      true
    }
  }

  /** Does this node have predecessors currently?
   *
   * @return true if this node has predecessors, false otherwise
   */
  def hasPredecessor: Boolean = _predecessors.nonEmpty

  /** Adds predecessor(s) associated with this node.
   *
   * @param predecessors the predecessor(s) to add
   * @return true if any duplicate predecessor was found, include those already added, false otherwise
   */
  def addPredecessors(predecessors: GraphNode*): Boolean = {
    val originalPredecessorsSize = this._predecessors.size
    predecessors.foreach(predecessor => {
      if (!this._predecessors.contains(predecessor)) this._predecessors += predecessor
    })
    originalPredecessorsSize != (this._predecessors.size - predecessors.size)
  }

  /** Adds predecessor(s) associated with this node.
    *
    * @param predecessor the predecessor(s) to add
    * @return true if the predecessor was not already added and added successfully, false otherwise
    */
  def addPredecessors(predecessor: Traversable[GraphNode]): Boolean = {
    addPredecessors(predecessor.toArray:_*)
  }

  /** Get the predecessors
   *
   * @return the current set of predecessors, if any
   */
  def predecessors: List[GraphNode] = _predecessors.toList
}
