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
package dagr.core.tasksystem

import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.execsystem.{ResourceSet, Scheduler}

/** A task that should be directly executed or scheduled.
 *
 * A few things about unit tasks:
 * - a task can depend on data from tasks on which it is dependent.  See [[Callbacks]].
 * - a task extending this class should return only one task in its getTasks method.  See [[Task.getTasks]].
 * - a task can perform any final logic dependent on the resources with which it is scheduled.  See [[Scheduler!.schedule*]].
  *
 * When a unit task gets scheduled, the [[dagr.core.execsystem.Scheduler.schedule]] method will be called to allow any final
 * logic based on the resources this task was scheduled with.  This is in addition to the steps listed in [[Task]].
 */
trait UnitTask extends Task with LazyLogging with Schedulable {
  /** Get the list of tasks to execute.
    *
    * For UnitTask and any class that extends it, if the task wishes to be directly executed,
    * the list should be of size one, and equal to itself.  If no resources have been specified,
    * both memory and cores default to the given resources.
    *
    * @return the list of tasks of to run.
    */
  final def getTasks: Traversable[_ <: this.type] = {
    List(this)
  }

  /**
    * Called by the Scheduler immediately prior to scheduling to allow tasks to perform any
    * necessary last-minute configuration with the knowledge of the exact set of resources
    * they are to be run with.
    */
  override def applyResources(resources: ResourceSet): Unit = Unit
}
