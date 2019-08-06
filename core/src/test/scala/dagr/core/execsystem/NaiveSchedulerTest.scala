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

import com.fulcrumgenomics.commons.CommonsDef._
import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.core.tasksystem._
import dagr.core.UnitSpec

import scala.collection.mutable.ListBuffer

class NaiveSchedulerTest extends UnitSpec with LazyLogging {
  private val scheduler = new NaiveScheduler()

  private val systemCores: Cores = Cores(2)
  private val systemMemory: Memory = Memory("2G")
  private val jvmMemory: Memory = Memory("2G")

  "NaiveScheduler" should "accept a simple task" in {
    val task: UnitTask = new NoOpTask withName "simple" requires(Cores(1), Memory("512M"))
    scheduler.schedule(List(task), systemCores, systemMemory, jvmMemory) should contain key task
  }

  it should "accept a simple task with maximum resources" in {
    val task: UnitTask = new NoOpTask withName "simple" requires(Cores(2), Memory("2G"))
    scheduler.schedule(List(task), systemCores, systemMemory, jvmMemory) should contain key task
  }

  it should "accept a simple task with no resources when the system has no resources" in {
    val task: UnitTask = new NoOpTask withName "simple" requires ResourceSet.empty
    scheduler.schedule(List(task), Cores.none, Memory.none, Memory.none) should contain key task
  }

  it should "reject a task for too much memory" in {
    val task: UnitTask = new NoOpTask withName "simple" requires(Cores(1), Memory("4G"))
    scheduler.schedule(List(task), systemCores, systemMemory, jvmMemory) should not contain task
  }

  it should "reject a simple task for too many cores" in {
    val task: UnitTask = new NoOpTask withName "simple" requires(Cores(3), Memory("2G"))
    scheduler.schedule(List(task), systemCores, systemMemory, jvmMemory) should not contain task
  }

  it should "accept two simple tasks" in {
    val taskOne: UnitTask = new NoOpTask withName "simple" requires(Cores(1), Memory("1G"))
    val taskTwo: UnitTask = new NoOpTask withName "simple" requires(Cores(1), Memory("1G"))
    val tasks: Map[UnitTask, ResourceSet] = scheduler.schedule(List(taskOne, taskTwo), systemCores, systemMemory, jvmMemory)
    tasks should contain key taskOne
    tasks should contain key taskTwo
    tasks should have size 2
  }

  it should "accept one of two simple tasks" in {
    val taskOne: UnitTask = new NoOpTask withName "simple" requires(Cores(1), Memory("2G"))
    val taskTwo: UnitTask = new NoOpTask withName "simple" requires(Cores(1), Memory("4G"))
    val tasks: Map[UnitTask, ResourceSet] = scheduler.schedule(List(taskOne, taskTwo), systemCores, systemMemory, jvmMemory)
    tasks should contain key taskOne
    tasks should have size 1
  }

  it should "accept the first task" in {
    val taskOne: UnitTask = new NoOpTask withName "simple" requires(Cores(1), Memory("1G"))
    val taskTwo: UnitTask = new NoOpTask withName "simple" requires(Cores(1), Memory("2G"))
    val tasks: Map[UnitTask, ResourceSet] = scheduler.schedule(List(taskOne, taskTwo), systemCores, systemMemory, jvmMemory)
    tasks should contain key taskOne
    tasks should have size 1
  }

  it should "accept the first task even when cores are unequal" in {
    val taskOne: UnitTask = new NoOpTask withName "simple" requires(Cores(1), Memory("2G"))
    val taskTwo: UnitTask = new NoOpTask withName "simple" requires(Cores(2), Memory("2G"))
    val tasks: Map[UnitTask, ResourceSet] = scheduler.schedule(List(taskOne, taskTwo), systemCores, systemMemory, jvmMemory)
    tasks should contain key taskOne
    tasks should have size 1
  }

  it should "accept two simple tasks with no resources with no system resources" in {
    val taskOne: UnitTask = new NoOpTask withName "simple" requires ResourceSet.empty
    val taskTwo: UnitTask = new NoOpTask withName "simple" requires ResourceSet.empty
    val tasks: Map[UnitTask, ResourceSet] = scheduler.schedule(List(taskOne, taskTwo), Cores.none, Memory.none, Memory.none)
    tasks should contain key taskOne
    tasks should contain key taskTwo
    tasks should have size 2
  }

  it should "accept two simple tasks, one with no resources" in {
    val taskOne: UnitTask = new NoOpTask withName "simple" requires(Cores(1), Memory("1G"))
    val taskTwo: UnitTask = new NoOpTask withName "simple" requires ResourceSet.empty
    val tasks = scheduler.schedule(List(taskOne, taskTwo), systemCores, systemMemory, jvmMemory)
    tasks should have size 2
    tasks should contain key taskOne
    tasks should contain key taskTwo
  }

  it should "accept only tasks with no resources set when there are no system resources" in {
    val taskOne: UnitTask = new NoOpTask withName "taskOne" requires(Cores(1), Memory("1G"))
    val taskTwo: UnitTask = new NoOpTask withName "taskTwo" requires ResourceSet.empty
    val tasks: Map[UnitTask, ResourceSet] = scheduler.schedule(List(taskOne, taskTwo), Cores.none, Memory.none, Memory.none)
    tasks should have size 1
    tasks should contain key taskTwo
  }

  it should "schedule a task with multiple possible resource sets" in {
    // A simple in-jvm task that can run with either 32M or 16M of memory
    val task: UnitTask = new InJvmTask {
      override def inJvmMethod(): Int = 0

      override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
        availableResources.subset(Cores(1), Memory("32M")) orElse
          availableResources.subset(Cores(1), Memory("16M"))
      }

      name = "in jvm task"
    }

    // Should schedule with 32M
    var scheduledTasksAndResources = scheduler.schedule(readyTasks = List(task), Cores(2), Memory.none, Memory("32M"))
    scheduledTasksAndResources should contain key task
    scheduledTasksAndResources should contain value ResourceSet(Cores(1), Memory("32M"))

    // Should schedule with 16M
    scheduledTasksAndResources = scheduler.schedule(readyTasks = List(task), Cores(2), Memory.none, Memory("16M"))
    scheduledTasksAndResources should contain key task
    scheduledTasksAndResources should contain value ResourceSet(Cores(1), Memory("16M"))

    // Should not schedule
    scheduledTasksAndResources = scheduler.schedule(readyTasks = List(task), Cores(2), Memory.none, Memory("8M"))
    scheduledTasksAndResources should not contain key(task)
    scheduledTasksAndResources should have size 0
  }

  it should "schedule system and in-Jvm tasks using separate memory" in {
    val readyTasks: ListBuffer[UnitTask] = new ListBuffer[UnitTask]()

    // Create three tasks of each type (system and in-Jvm) such that their resources are half of the total available
    // resources.  This way, two of each should be scheduled.
    for (i <- 1 to 3) {
      readyTasks += new ShellCommand("exit 0") withName "system task" requires(Cores(1), Memory("1G"))
      readyTasks += new NoOpInJvmTask(taskName = "in jvm task") requires(Cores(1), Memory("16M"))
    }
    val systemCores: Cores = Cores(4)
    val systemMemory: Memory = Memory("4G")
    val jvmMemory: Memory = Memory("32M")

    // schedule
    val scheduledTasksAndResources = scheduler.schedule(readyTasks = readyTasks.toList, systemCores, systemMemory, jvmMemory)

    scheduledTasksAndResources.keys.filter(task => task.isInstanceOf[ProcessTask]) should have size 2
    scheduledTasksAndResources.keys.filter(task => task.isInstanceOf[InJvmTask]) should have size 2
  }

  it should "not schedule tasks concurrently with more Cores than are defined in the system." in {
    val scheduler = new NaiveScheduler()
    val systemCores: Cores = Cores(4)
    val systemMemory: Memory = Memory("4G")
    val jvmMemory: Memory = Memory.none

    // A task that would like 1-8 cores each
    class HungryTask extends ProcessTask {
      override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
        availableResources.subset(minCores = Cores(1), maxCores = Cores(8), memory = Memory("1g"))
      }

      override val args = List.empty
    }
    val readyTasks = List(new HungryTask, new HungryTask, new HungryTask)

    val tupleOption = scheduler.scheduleOneTask(readyTasks, systemCores, systemMemory, jvmMemory)
    tupleOption.isDefined shouldBe true
    val resourceSet = tupleOption.get._2
    resourceSet.cores.value should be <= systemCores.value
  }

  class NotATask extends UnitTask {
    override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = None
  }

  it should "throw an IllegalArgumentException when neither a ProcessTask nor a InJvmTask is given for runningTasks." in {
    an[java.lang.IllegalArgumentException] should be thrownBy scheduler.schedule(
      runningTasks = Map[UnitTask, ResourceSet](new NotATask -> ResourceSet(1, 1)),
      readyTasks = Nil,
      systemCores = Cores(1),
      systemMemory = Memory(1),
      jvmMemory = Memory(1)
    )
  }

  it should "throw an IllegalArgumentException when neither a ProcessTask nor a InJvmTask is given for readyTasks." in {
    an[java.lang.IllegalArgumentException] should be thrownBy scheduler.schedule(
      runningTasks = Map.empty[UnitTask, ResourceSet],
      readyTasks = Seq(new NotATask),
      systemCores = Cores(1),
      systemMemory = Memory(1),
      jvmMemory = Memory(1)
    )
  }
}
