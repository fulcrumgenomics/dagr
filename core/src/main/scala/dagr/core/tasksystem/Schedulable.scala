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
package dagr.core.tasksystem

import dagr.core.execsystem.{Cores, Memory, ResourceSet}

/**
  * Traits that isolates methods about how Tasks interact with the Scheduler, and allows
  * for multiple implementations independent from the Task hierarchy.
  */
trait Schedulable {
  /**
    * Given a non-null ResourceSet representing the available resources at this moment in time
    * return either a ResourceSet that is a subset of the available resources in which the task
    * can run, or None if the task cannot run in the available resources.
    *
    * @param availableResources The system resources available to the task
    * @return Either a ResourceSet of the desired subset of resources to run with, or None
    */
  def pickResources(availableResources: ResourceSet): Option[ResourceSet]

  /**
    * Called by the Scheduler immediately prior to execution to allow tasks to perform any
    * necessary last-minute configuration with the knowledge of the exact set of resources
    * they are to be run with.
    *
    * @param resources the set of resources that the task will be run with
    */
  def applyResources(resources : ResourceSet): Unit
}

/**
  * A trait for all tasks that by default have an empty resource set.
  */
private[tasksystem] trait ScheduleWithEmptyDefaultResources extends Schedulable {
  private[tasksystem] var resourceSet = ResourceSet.empty

  /** Provides access to the currently allocated set of resources for the task. */
  def resources: ResourceSet = this.resourceSet
}

/**
  * A trait for all tasks that required a specific number of cores and a specific amount of memory.
  * The amount can be decided any time prior to takeResources() being called, but must be a single
  * set of values.
  */
trait FixedResources extends ScheduleWithEmptyDefaultResources {
  requires(ResourceSet(Cores(1), Memory("32M")))

  /** Sets the resources that are required by this task, overriding all previous values. */
  def requires(resources: ResourceSet) : this.type = {
    this.resourceSet = resources
    this
  }

  /** Sets the resources that are required by this task, overriding all previous values. */
  def requires(cores: Cores = Cores.none, memory: Memory = Memory.none) : this.type = requires(ResourceSet(cores, memory))

  /** Sets the resources that are required by this task, overriding all previous values. */
  def requires(cores: Double, memory: String) : this.type = { requires(Cores(cores), Memory(memory)) }

  /**
    * Implemented to take the the fixed amount of cores and memory from the provided resource set.
    */
  override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = availableResources.subset(this.resources)
}

trait VariableResources extends ScheduleWithEmptyDefaultResources {

  override def applyResources(resources: ResourceSet): Unit = this.resourceSet = resources
}
