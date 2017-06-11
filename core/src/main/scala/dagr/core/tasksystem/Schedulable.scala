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

import dagr.core.exec.{Cores, Memory, Resource, ResourceSet}

object Schedulable {

  /** The multiple of the system resources to use when searching for the resources with which a task can execute. */
  private val ResourceMultiple: Int = 16

  /** The maximum # cores to use when searching for the resources with which a task can execute. */
  private[tasksystem] val EndCores: Cores = {
    val cores    = Resource.systemCores.value * ResourceMultiple
    val minCores = 64
    if (cores < minCores) Cores(minCores) else Cores(cores)
  }

  /** The maximum amount of memory to use when searching for the resources with which a task can execute. */
  private[tasksystem] val EndMemory: Memory = {
    val memory    = Resource.systemMemory.value * ResourceMultiple
    val minMemory = Memory("256g")
    if (memory < minMemory.value) minMemory else Memory(memory)
  }

  /** The maximum resources to use when searching for the resources with which a task can execute. */
  private[tasksystem] val EndResources: ResourceSet = new ResourceSet(cores=EndCores, memory=EndMemory)

  /** The amount of memory per core when searching for the memory with which a task can execute. */
  private[tasksystem] val MemoryPerCore = Memory("1g")

  /** The minimum amount of memory to start when searching for the memory with which a task can execute. */
  private[tasksystem] val StartMemory = Memory("256m")

  /** The minimum amount of memory to increase the memory when searching for the memory with which a task can execute. */
  private[tasksystem] val StepMemory: Memory = {
    val stepMemory = Memory(EndMemory.value / 100)
    if (stepMemory < StartMemory) stepMemory
    else StartMemory
  }

  /** A single core! */
  private[tasksystem] val OneCore: Cores = Cores(1)
}

/**
  * Traits that isolates methods about how Tasks interact with the Scheduler, and allows
  * for multiple implementations independent from the Task hierarchy.
  */
trait Schedulable {
  
  import Schedulable._
  
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

  /** A crude attempt at estimating the minimum cores needed by assuming a fixed maximum memory. */
  private[tasksystem] def minCores(start: Cores = OneCore, end: Cores = EndCores, step: Cores = OneCore, maxMemory: Memory = EndMemory): Option[Cores] = {
    // first check that the search will succeed, then test a range of values for cores
    this.pickResources(availableResources = new ResourceSet(end, maxMemory)).flatMap { _ =>
      (start.value to end.value by step.value).flatMap { cores =>
        val availableResources = new ResourceSet(Cores(cores), maxMemory)
        this.pickResources(availableResources = availableResources).map(_.cores)
      }.headOption
    }
  }

  /** A crude attempt at estimating the minimum memory needed by assuming a fixed maximum number of cores. */
  private[tasksystem] def minMemory(start: Memory = StartMemory, end: Memory = EndMemory, step: Memory = StepMemory, maxCores: Cores = EndCores): Option[Memory] = {
    // first check that the search will succeed, then test a range of values for memory
    this.pickResources(availableResources = new ResourceSet(maxCores, end)).flatMap { _ =>
      (start.value to end.value by step.value).flatMap { memory =>
        val availableResources = new ResourceSet(maxCores, Memory(memory))
        this.pickResources(availableResources = availableResources).map(_.memory)
      }.headOption
    }
  }

  /** A crude attempt at estimating the minimum resources by assuming a fixed ratio of memory per core. */
  private[tasksystem] def minCoresAndMemory(start: Cores = OneCore, end: Cores = EndCores, step: Cores = OneCore, memoryPerCore: Memory = MemoryPerCore): Option[ResourceSet] = {
    (start.value to end.value by step.value).flatMap { cores =>
      val availableResources = new ResourceSet(Cores(cores), Memory((memoryPerCore.value * cores).toLong))
      this.pickResources(availableResources = availableResources)
    }.headOption
  }

  /**
    * Given a non-null ResourceSet representing the maximum available resources return either a
    * ResourceSet that is a subset of the available resources in which the task can run, or None
    * if the task cannot run in the maximum resources.
    *
    * First estimates the # of cores by assuming an infinite amount of memory, and then estimates
    * the amount of memory assuming an infinite # of cores.  If either does not yield a value or if
    * the task cannot run with the combination of the two, then try a various combinations of cores
    * and memory where we assume a fixed ratio between cores and memory (ex. 1g per core).
    *
    * @param maximumResources The maximum system resources available to the task.
    * @return Either a ResourceSet of the subset of resources that this task can run with, or None
    */
  private[core] def minResources(maximumResources: ResourceSet = new ResourceSet(cores=EndCores, memory=EndMemory)): Option[ResourceSet] = {
    val cores  = minCores(end=maximumResources.cores, maxMemory=maximumResources.memory)
    val memory = minMemory(end=maximumResources.memory, maxCores=maximumResources.cores)
    (cores, memory) match {
      case (Some(c), Some(m)) => this.pickResources(maximumResources.copy(cores=c, memory=m))
      case _                  =>
        // try a combination of cores and memory with a fixed ratio between the two.
        val startMemoryPerCore = StartMemory.value
        val endMemoryPerCore   = maximumResources.memory.value
        val stepMemoryPerCore  = StepMemory.value
        (startMemoryPerCore to endMemoryPerCore by stepMemoryPerCore).flatMap { memoryPerCore =>
          this.minCoresAndMemory(end=maximumResources.cores, memoryPerCore=Memory(memoryPerCore)).flatMap(this.pickResources)
        }.headOption
    }
  }
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
  import Schedulable.{EndResources, OneCore}

  requires(ResourceSet(OneCore, Memory("32M")))

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
    * Implemented to take the fixed amount of cores and memory from the provided resource set.
    */
  override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = availableResources.subset(this.resources)

  /**
    * Given a non-null ResourceSet representing the maximum available resources return either a
    * ResourceSet that is a subset of the available resources in which the task can run, or None
    * if the task cannot run in the maximum resources.
    *
    * Since a the task has a fixed amount of resources, returns None if there is not enough resources,
    * or the fixed amount of cores and memory from the provided resource set.
    *
    * @param maximumResources The maximum system resources available to the task.
    * @return Either a ResourceSet of the subset of resources that this task can run with, or None
    */
  override private[core] def minResources(maximumResources: ResourceSet = EndResources): Option[ResourceSet] = {
    this.pickResources(maximumResources)
  }
}

trait VariableResources extends ScheduleWithEmptyDefaultResources {

  override def applyResources(resources: ResourceSet): Unit = this.resourceSet = resources
}
