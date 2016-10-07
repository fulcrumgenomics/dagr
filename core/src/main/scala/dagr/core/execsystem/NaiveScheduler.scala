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

import dagr.core.tasksystem.{InJvmTask, ProcessTask, UnitTask}
import SchedulingStrategy._
import dagr.commons.CommonsDef.unreachable

/** A little trait to help choose a task to be scheduled among a set of tasks whose resource needs can be met. */
trait SelectTaskScheduler {
  /**
    * Selects a task to be scheduled among a set of tasks whose resource needs can be met.
 *
    * @param tasksAndResources the task its associated resource set.
    * @return
    */
  protected def selectTask(tasksAndResources: Traversable[(UnitTask, ResourceSet)]): Option[(UnitTask, ResourceSet)]
}

/** Selects the first task encountered regardless. */
trait SelectFirstTaskScheduler extends SelectTaskScheduler {
  override protected def selectTask(tasksAndResources: Traversable[(UnitTask, ResourceSet)]): Option[(UnitTask, ResourceSet)] = tasksAndResources.headOption
}

/** Selects the task with the smallest Task Id. */
trait SelectMinimumIdTaskScheduler extends SelectTaskScheduler {
  override protected def selectTask(tasksAndResources: Traversable[(UnitTask, ResourceSet)]): Option[(UnitTask, ResourceSet)] = {
    tasksAndResources.toSeq
      .sortBy(_._1.taskInfo.taskId)
      .headOption
  }
}

/** Selects the task needing the most cores. */
trait SelectMaximumCoresTaskScheduler extends SelectTaskScheduler {
  override protected def selectTask(tasksAndResources: Traversable[(UnitTask, ResourceSet)]): Option[(UnitTask, ResourceSet)] = {
    tasksAndResources.toSeq
      .sortBy(_._2.cores.value)
      .lastOption
  }
}

/** Selects the task needing the most memory. */
trait SelectMaximumMemoryTaskScheduler extends SelectTaskScheduler {
  override protected def selectTask(tasksAndResources: Traversable[(UnitTask, ResourceSet)]): Option[(UnitTask, ResourceSet)] = {
    tasksAndResources.toSeq
      .sortBy(_._2.memory.value)
      .lastOption
  }
}

object NaiveScheduler {
  /** Create a naive scheduler with the provided strategy to select among tasks whose resources needs can be met. */
  def apply(strategy: SchedulingStrategy = AnyTask): NaiveScheduler = {
    strategy match {
      case AnyTask   => new NaiveScheduler with SelectFirstTaskScheduler
      case MinTaskId => new NaiveScheduler with SelectMinimumIdTaskScheduler
      case MaxCores  => new NaiveScheduler with SelectMaximumCoresTaskScheduler
      case MaxMemory => new NaiveScheduler with SelectMaximumMemoryTaskScheduler
      case _         => unreachable("Unknown scheduling strategy")
    }
  }
}

/** Simple scheduler that picks the task that uses the most memory, cores, then disk. */
abstract class NaiveScheduler extends Scheduler with SelectTaskScheduler {
  /**
    * Takes the list of tasks that could be scheduled if their resource needs can be met and attempts
    * to schedule a single task for execution.
    */
  private[execsystem] def scheduleOneTask(readyTasks: Traversable[UnitTask],
                                          remainingSystemCores: Cores,
                                          remainingSystemMemory: Memory,
                                          remainingJvmMemory: Memory): Option[(UnitTask, ResourceSet)] = {
    val systemResourceSet: ResourceSet = ResourceSet(remainingSystemCores, remainingSystemMemory)
    val jvmResourceSet: ResourceSet = ResourceSet(remainingSystemCores, remainingJvmMemory)
    // Find a task that can be executed
    val schedulableTasks = readyTasks
      .view // lazy
      .map { // pick resources
        case task: ProcessTask => (task, task.pickResources(systemResourceSet))
        case task: InJvmTask   => (task, task.pickResources(jvmResourceSet))
      }
      .flatMap { // find those that returned a resource set
        case (task, Some(resourceSet)) => Some((task, resourceSet))
        case _ => None
      }
    selectTask(schedulableTasks)
  }

  /** Runs one round of scheduling, trying to schedule as many ready tasks as possible given the
    * resources.  Each time, it chooses the first task that can be executed with the available
    * resources.
    *
    * @param readyTasks the tasks that should be considered to be schedule.
    * @param remainingSystemCores the set of remaining system cores, not including running tasks.
    * @param remainingSystemMemory the set of remaining system memory, not including running tasks.
    * @param remainingJvmMemory the set of remaining JVM memory, not including running tasks.
    * @return a map of tasks should be scheduled and their allocate resources.
    */
  private def scheduleOnce(readyTasks: Traversable[UnitTask],
                           remainingSystemCores: Cores,
                           remainingSystemMemory: Memory,
                           remainingJvmMemory: Memory): List[(UnitTask, ResourceSet)] = {
    // no more tasks ready to be scheduled
    if (readyTasks.isEmpty) Nil
    else {
      logger.debug(s"the resources were [System cores=" + remainingSystemCores.value
        + " System memory=" + Resource.parseBytesToSize(remainingSystemMemory.value)
        + " JVM memory=" + Resource.parseBytesToSize(remainingJvmMemory.value) + " ]")

      // try one round of scheduling, and recurse if a task could be scheduled
      scheduleOneTask(readyTasks, remainingSystemCores, remainingSystemMemory, remainingJvmMemory) match {
        case None =>
          Nil
        case Some((task: UnitTask, resourceSet: ResourceSet)) =>
          logger.debug("task to schedule is [" + task.name + "]")
          logger.debug(s"task [${task.name}] uses the following resources [" + resourceSet + "]")
          List[(UnitTask, ResourceSet)]((task, resourceSet)) ++ (task match {
            case processTask: ProcessTask =>
              scheduleOnce(
                readyTasks = readyTasks.filterNot(t => t == task),
                remainingSystemCores = remainingSystemCores - resourceSet.cores,
                remainingSystemMemory = remainingSystemMemory - resourceSet.memory,
                remainingJvmMemory = remainingJvmMemory
              )
            case inJvmTask: InJvmTask =>
              scheduleOnce(
                readyTasks = readyTasks.filterNot(t => t == task),
                remainingSystemCores = remainingSystemCores - resourceSet.cores,
                remainingSystemMemory = remainingSystemMemory,
                remainingJvmMemory = remainingJvmMemory - resourceSet.memory
              )
          })
      }
    }
  }

  /** Schedule a task.  Picks a task that uses the most memory, cores, then disk.  All tasks should have
   * a resource matching each system resource.
   *
   * @param readyTasks the tasks that should be considered to be schedule.
   * @param systemCores the system cores available.
   * @param systemMemory the system memory available.
   * @param jvmMemory the JVM memory available.
   * @return the map of tasks should be scheduled.
   */
  override def schedule(readyTasks: Traversable[UnitTask],
                        systemCores: Cores,
                        systemMemory: Memory,
                        jvmMemory: Memory
                       ): Map[UnitTask, ResourceSet] = {
    val tasksToSchedule = scheduleOnce(readyTasks, systemCores, systemMemory, jvmMemory)
    logger.debug("scheduling " + tasksToSchedule.size + " tasks")
    tasksToSchedule.toMap
  }
}
