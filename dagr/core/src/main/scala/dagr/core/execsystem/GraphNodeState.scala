/*
 * The MIT License
 *
 * Copyright (c) 2015 Fulcrum Genomics LLC
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

import dagr.core.tasksystem.Task

/** The state for a [[GraphNode]]. */
object GraphNodeState extends Enumeration {
  type GraphNodeState = Value
  val ORPHAN, /** has predecessors that have not been seen by the system */
  PREDECESSORS_AND_UNEXPANDED, /** has predecessors and [[Task.getTasks()]] has not been called */
  ONLY_PREDECESSORS, /** has predecessors after [[dagr.core.tasksystem.Task.getTasks()]] was called */
  NO_PREDECESSORS, /** no predecessors, eligible for scheduling */
  RUNNING, /** is running */
  COMPLETED /** is completed */
  = Value

  /** Checks if the state means we have predecessors.
   *
   * @param state the state to check.
   * @return true if the state implies that the node has predecessors, false otherwise
   */
  def hasPredecessors(state: GraphNodeState.Value): Boolean = state == PREDECESSORS_AND_UNEXPANDED || state == ONLY_PREDECESSORS
}
