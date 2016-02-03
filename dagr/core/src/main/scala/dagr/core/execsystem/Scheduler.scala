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

import dagr.core.tasksystem.{InJvmTask, ProcessTask, Task, UnitTask}
import dagr.core.util.LazyLogging

/** Utility methods for scheduling tasks. */
object Scheduler {
  /** Check if a task can be scheduled given the system resources.  This will ignore task resources
   * that are not found in system resources but requires the opposite.
   *
   * @param task the task to schedule.
   * @param availableResources the set of  system resources.
   * @return true if there are enough system resources for the given task.
   */
  def canScheduleTask(task: UnitTask, availableResources: ResourceSet): Boolean = task.pickResources(availableResources).isDefined
}

/** Scheduler of [[Task]] tasks */
abstract class Scheduler extends LazyLogging {

  def schedule(runningTasks: Map[UnitTask, ResourceSet],
               readyTasks: Traversable[UnitTask],
               systemCores: Cores,
               systemMemory: Memory,
               jvmMemory: Memory): Map[UnitTask, ResourceSet] =  {

    // Make sure we either have tasks that inherit from ProcessTask or InJvmTask.
    runningTasks.keys.foreach(task => {
      if (!task.isInstanceOf[ProcessTask] && !task.isInstanceOf[InJvmTask]) {
        throw new IllegalArgumentException(s"The running task was neither an InJvmTask nor ProcessTask: name[${task.name}] class[${task.getClass.getSimpleName}]")
      }
    })
    readyTasks.foreach(task => {
      if (!task.isInstanceOf[ProcessTask] && !task.isInstanceOf[InJvmTask]) {
        throw new IllegalArgumentException(s"The ready task was neither an InJvmTask nor ProcessTask: name[${task.name}] class[${task.getClass.getSimpleName}]")
      }
    })

    // Get the remaining resources
    val remainingSystemCores: Cores = Cores(
      systemCores.value - runningTasks.values.map(_.cores.value).sum
    )
    val remainingSystemMemory: Memory = Memory(
      systemMemory.value - runningTasks.filterKeys(_.isInstanceOf[ProcessTask]).values.map(_.memory.value).sum
    )
    runningTasks.filterKeys(_.isInstanceOf[InJvmTask])
    val remainingJvmMemory: Memory = Memory(
      jvmMemory.value - runningTasks.filterKeys(_.isInstanceOf[InJvmTask]).values.map(_.memory.value).sum
    )

    // Schedule
    schedule(readyTasks, remainingSystemCores, remainingSystemMemory, remainingJvmMemory)
  }

  protected def schedule(readyTasks: Traversable[UnitTask],
                         systemCores: Cores,
                         systemMemory: Memory,
                         jvmMemory: Memory): Map[UnitTask, ResourceSet]
}
