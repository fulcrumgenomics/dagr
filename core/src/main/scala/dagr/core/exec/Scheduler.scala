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
 *
 */
package dagr.core.exec

import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.tasksystem.{InJvmTask, ProcessTask, Task, UnitTask}

/** Scheduler of [[Task]] tasks */
abstract class Scheduler extends LazyLogging {

  /** Schedule tasks for execution with a given set of resources.  Returns a map of scheduled tasks to their resources to use
    * while running.  Only schedules based on the resources available after subtracting resources from running tasks.
    *
    * @param runningTasks the tasks that are currently running.
    * @param readyTasks the tasks that should be considered to be scheduled.
    * @param systemCores the set of system cores.
    * @param systemMemory the set of system memory.
    * @param jvmMemory the set of JVM memory.
    * @return a map of tasks should be scheduled and their allocate resources.
    * */
  final def schedule(runningTasks: Map[UnitTask, ResourceSet],
               readyTasks: Traversable[UnitTask],
               systemCores: Cores,
               systemMemory: Memory,
               jvmMemory: Memory): Map[UnitTask, ResourceSet] =  {

    // Make sure we either have tasks that inherit from ProcessTask or InJvmTask.
    runningTasks.keys.foreach(task => {
      if (!task.isInstanceOf[ProcessTask] && !task.isInstanceOf[InJvmTask]) {
        throw new IllegalArgumentException(s"The running task was neither an InJvmTask nor ProcessTask: " +
          s"name[${task.name}] class[${task.getClass.getSimpleName}]")
      }
    })
    readyTasks.foreach(task => {
      if (!task.isInstanceOf[ProcessTask] && !task.isInstanceOf[InJvmTask]) {
        throw new IllegalArgumentException(s"The ready task was neither an InJvmTask nor ProcessTask: " +
          s"name[${task.name}] class[${task.getClass.getSimpleName}]")
      }
    })

    // Get the remaining resources
    val remainingSystemCores: Cores = Cores(
      systemCores.value - runningTasks.values.map(_.cores.value).sum
    )
    val remainingSystemMemory: Memory = Memory(
      systemMemory.value - runningTasks.filterKeys(_.isInstanceOf[ProcessTask]).values.map(_.memory.value).sum
    )
    val remainingJvmMemory: Memory = Memory(
      jvmMemory.value - runningTasks.filterKeys(_.isInstanceOf[InJvmTask]).values.map(_.memory.value).sum
    )

    // Schedule
    schedule(readyTasks, remainingSystemCores, remainingSystemMemory, remainingJvmMemory)
  }

  /** All schedulers should implement this method.  Returns a map of scheduled tasks to their resources to use
    * while running.
    */
  protected def schedule(readyTasks: Traversable[UnitTask],
                         systemCores: Cores,
                         systemMemory: Memory,
                         jvmMemory: Memory): Map[UnitTask, ResourceSet]
}
