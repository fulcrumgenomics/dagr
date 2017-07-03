/*
 * The MIT License
 *
 * Copyright (c) 2017 Fulcrum Genomics LLC
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

import java.io.{OutputStream, PrintStream}
import java.nio.file.Files

import dagr.core.FutureUnitSpec
import dagr.core.reporting.ExecutionLogger.{Definition, Relationship, Status}
import dagr.core.tasksystem._
import com.fulcrumgenomics.commons.CommonsDef.unreachable
import com.fulcrumgenomics.commons.io.Io
import com.fulcrumgenomics.commons.util.Logger
import dagr.core.reporting.ExecutionLogger
import dagr.core.tasksystem.Task.TaskStatus

class SimpleTaskCacheTest extends FutureUnitSpec {

  /** The executor set on each task, so that we know which set of statuses to check */
  private val executor: Executor = Executor(experimentalExecution=true, resources=SystemResources.infinite)

  /** The status with the lowest ordinal that does not represent a successful execution status. */
  private val failStatus: TaskStatus = Stream.from(0).map(i => executor.statusFrom(i)).find(!_.success).getOrElse(unreachable("No unsuccessful status found"))

  /** The status with the lowest ordinal that represents a successful execution status. */
  private val successfulStatus: TaskStatus  = Stream.from(0).map(i => executor.statusFrom(i)).find(_.success).getOrElse(unreachable("No successful status found"))

  /** A simple implicit to set the default executor */
  implicit private class WithExecutor[T <: Task](task: T) {
    def withExecutor: T = { task._executor = Some(executor); task }
  }

  private def task: Task = new NoOpTask withExecutor

  private def anonTask: Task = new NoOpTask { } withExecutor

  "SimpleTaskCache" should "fail if a task has more than one definition in the replay log" in {
    val definition = Definition(new NoOpTask, 1, 1)
    val lines      = Seq(definition, definition).map(_.toString)
    val exception  = intercept[Exception] { new SimpleTaskCache(lines) }
    exception.getMessage should include ("had 2 definitions")
  }

  it should "fail if a parent and child have more than one relationship defined" in {
    val parent       = Definition.buildRootDefinition(new NoOpTask)
    val child        = Definition(new NoOpTask, parent.code, 0)
    val relationship = Relationship(parent, child)
    val lines        = Seq(relationship, relationship, parent, child).map(_.toString)
    val exception    = intercept[Exception] { new SimpleTaskCache(lines) }
    exception.getMessage should include ("had 2 relationships")
  }

  it should "fail if a parent or child definition is missing given a relationship between the two" in {
    val parent       = Definition.buildRootDefinition(new NoOpTask)
    val child        = Definition(new NoOpTask, parent.code, 0)
    val relationship = Relationship(parent, child)

    // Missing parent
    {
      val lines        = Seq(child, relationship).map(_.toString)
      val exception    = intercept[Exception] { new SimpleTaskCache(lines) }
      exception.getMessage should include (s"missing a definition for task '${parent.code}' for relationship '$relationship'")
    }

    // Missing child
    {
      val lines        = Seq(parent, relationship).map(_.toString)
      val exception    = intercept[Exception] { new SimpleTaskCache(lines) }
      exception.getMessage should include (s"missing a definition for task '${child.code}' for relationship '$relationship'")
    }
  }

  it should "fail if a task definition is not found for a status" in {
    val definition = Definition.buildRootDefinition(new NoOpTask)
    val status     = Status(executor.statusFrom(0), definition)
    val lines      = Seq(status).map(_.toString)
    val exception  = intercept[Exception] { new SimpleTaskCache(lines) }
    exception.getMessage should include (s"missing a definition for task '${definition.code}' with status '$status'")
  }

  it should "fail if parent definition does not exist for all definitions except the root" in {
    val root         = Definition.buildRootDefinition(new NoOpTask)
    val parent       = Definition(new NoOpTask, root.code, 0)
    val child        = Definition(new NoOpTask, parent.code, 0)
    val lines        = Seq(root, child).map(_.toString)
    val exception    = intercept[Exception] { new SimpleTaskCache(lines) }
    exception.getMessage should include ("Parent definition not found for")
  }

  it should "fail if not one and only one definition for a root task was found" in {
    val root = Definition.buildRootDefinition(new NoOpTask)

    // No roots
    {
      val exception = intercept[Exception] { new SimpleTaskCache(Seq.empty) }
      exception.getMessage should include("Did not find a definition for a root task")
    }

    // Two roots
    {
      val altRoot   = Definition.buildRootDefinition(new NoOpTask)
      val lines     = Seq(root, altRoot).map(_.toString)
      val exception = intercept[Exception] { new SimpleTaskCache(lines) }
      exception.getMessage should include ("Found multiple definitions for a root task")
    }
  }

  "SimpleTaskCache.register" should "register a root task with no children successfully" in {
    val root       = task
    val definition = Definition.buildRootDefinition(root)

    Seq(failStatus, successfulStatus).foreach { execStatus: TaskStatus =>
      val status = Status(execStatus, definition)
      val lines  = Seq(definition, status).map(_.toString)
      val cache  = new SimpleTaskCache(lines)
      cache.register(root)
      cache.tasks contains root
      cache.execute(root) shouldBe false // always false, since we need to call register with at least one child task
      cache.register(root, root)
      cache.execute(root) shouldBe !execStatus.success
    }
  }

  it should "register a root task with children successfully" in {
    Seq(failStatus, successfulStatus).foreach { execStatus: TaskStatus =>
      val root       = task
      val rootDef    = Definition.buildRootDefinition(root)
      val child      = task
      val childDef   = Definition(child, rootDef.code, 0)
      val rootStatus = Status(execStatus, rootDef)
      val lines      = Seq(rootDef, childDef, rootStatus, Relationship(rootDef, childDef)).map(_.toString)
      val cache = new SimpleTaskCache(lines)
      cache.register(root) // root
      cache.register(root, child)
      cache.execute(root) shouldBe !execStatus.success
    }
  }

  it should "fail if the parent of a task was not previously registered" in {
    val parent    = task
    val parentDef = Definition.buildRootDefinition(parent)
    val child     = task
    val childDef  = Definition(child, parentDef.code, 0)
    val lines     = Seq(parentDef, childDef, Relationship(parentDef, childDef)).map(_.toString)
    val cache     = new SimpleTaskCache(lines)

    // Unit task
    {
      val exception = intercept[Exception] { cache.register(child, child) }
      exception.getMessage should include("Task was not previously registered")
    }

    // Parent built the child
    {
      val exception = intercept[Exception] { cache.register(parent, child)}
      exception.getMessage should include("Parent was not defined when registering its children")
    }
  }

  it should "register a unit task (a task with parent == child)" in {
    val root     = task
    val rootDef  = Definition.buildRootDefinition(root)
    val child    = task
    val childDef = Definition(child, rootDef.code, 0)
    val lines    = Seq(rootDef, childDef, Relationship(rootDef, childDef)).map(_.toString)

    // Just the root
    {
      val cache = new SimpleTaskCache(lines)
      cache.register(root) // root
      cache.register(root, root)
      cache.tasks should contain theSameElementsAs Seq(root)
      cache.execute(root) shouldBe true // no status found!
    }

    // Just the child
    {
      val cache = new SimpleTaskCache(Status(successfulStatus, rootDef).toString +: lines)
      cache.register(root) // root
      cache.register(root, child)
      cache.register(child, child)
      cache.tasks should contain theSameElementsAs Seq(root, child)
      cache.execute(root) shouldBe false // previously successful
      cache.execute(child) shouldBe true // no status found
    }
  }

  it should "execute if a parent has a different # of children than in the replay log" in {
    val root              = task
    val rootDef           = Definition.buildRootDefinition(root)
    val child             = task
    val childDef          = Definition(child, rootDef.code, 0)
    val extraChild        = anonTask
    val extraChildDef     = Definition(extraChild, rootDef.code, 1)
    val relationship      = Relationship(rootDef, childDef)
    val extraRelationship = Relationship(rootDef, extraChildDef)

    // current has zero, replay log has one, the previous status was success
    {
      val lines = Seq(rootDef, childDef, relationship, Status(successfulStatus, rootDef)).map(_.toString)
      val cache = new SimpleTaskCache(lines)
      cache.register(root) // register the root task
      cache.register(root, root) // register the root task as a unit task!
      cache.tasks should contain theSameElementsAs Seq(root)
      cache.execute(root) shouldBe true
    }

    // current has one, replay log has zero
    {
      val lines = Seq(rootDef, Status(successfulStatus, rootDef)).map(_.toString)
      val cache = new SimpleTaskCache(lines)
      cache.register(root) // register the root task
      cache.register(root, child)
      cache.tasks should contain theSameElementsAs Seq(root, child)
      cache.execute(root) shouldBe true
    }

    // current has one, replay log has two
    {
      val lines = Seq(rootDef, childDef, extraChildDef, relationship, extraRelationship, Status(successfulStatus, rootDef)).map(_.toString)
      val cache = new SimpleTaskCache(lines)
      cache.register(root) // register the root task
      cache.register(root, child)
      cache.tasks should contain theSameElementsAs Seq(root, child)
      cache.execute(root) shouldBe true
    }

    // current has two, replay log has one
    {
      val lines = Seq(rootDef, childDef, relationship, Status(successfulStatus, rootDef)).map(_.toString)
      val cache = new SimpleTaskCache(lines)
      cache.register(root) // register the root task
      cache.register(root, child, extraChild)
      cache.tasks should contain theSameElementsAs Seq(root, child, extraChild)
      cache.execute(root) shouldBe true
    }
  }

  it should "execute if the child has a different name" in {
    val root           = task
    val rootDef        = Definition.buildRootDefinition(root)
    val child          = task
    val replayChildDef = Definition(child, rootDef.code, 0).copy(simpleName="None")
    val relationship   = Relationship(rootDef, replayChildDef)

    val lines = Seq(rootDef, replayChildDef, relationship, Status(successfulStatus, rootDef)).map(_.toString)
    val cache = new SimpleTaskCache(lines)
    cache.register(root) // register the root task
    cache.register(root, child)
    cache.tasks should contain theSameElementsAs Seq(root, child)
    cache.execute(root) shouldBe true
  }

  it should "execute if the child has a different child number" in {
    val root              = task
    val rootDef           = Definition.buildRootDefinition(root)
    val child             = task
    val childDef          = Definition(child, rootDef.code, 0)
    val extraChild        = anonTask
    val extraChildDef     = Definition(extraChild, rootDef.code, 1)
    val relationship      = Relationship(rootDef, childDef)
    val extraRelationship = Relationship(rootDef, extraChildDef)

    val lines = Seq(rootDef, childDef, extraChildDef, relationship, extraRelationship, Status(successfulStatus, rootDef)).map(_.toString)
    val cache = new SimpleTaskCache(lines)
    cache.register(root) // register the root task
    cache.register(root, extraChild, child) // children in a different order!
    cache.execute(root) shouldBe true
    cache.register(child, child)
    cache.execute(child) shouldBe true
    cache.register(extraChild, extraChild)
    cache.execute(extraChild) shouldBe true
  }

  it should "execute if the child it cannot be found in the replay log" in {
    val root              = task
    val rootDef           = Definition.buildRootDefinition(root)
    val child             = task
    val childDef          = Definition(child, rootDef.code, 0)
    val extraChild        = anonTask
    val relationship      = Relationship(rootDef, childDef)

    // child is in the replay log, but is replaced by extraChild
    val lines = Seq(rootDef, childDef, relationship, Status(successfulStatus, rootDef)).map(_.toString)
    val cache = new SimpleTaskCache(lines)
    cache.register(root) // register the root task
    cache.register(root, extraChild) // children in a different order!
    cache.execute(root) shouldBe true // its children cannot be found
    cache.register(extraChild, extraChild)
    cache.execute(extraChild) shouldBe true
  }

  it should "execute a child if the parent is not found in the replay log" in {
    val root       = task
    val rootDef    = Definition.buildRootDefinition(root)
    val parent     = task
    val child      = task
    val extraChild = task

    val lines = Seq(rootDef, Status(successfulStatus, rootDef)).map(_.toString)
    val cache = new SimpleTaskCache(lines)
    cache.register(root) // register the root task
    cache.register(root, parent)
    cache.execute(root) shouldBe true // its child (parent) cannot be found, so execute it
    cache.register(parent, child)
    cache.execute(parent) shouldBe true // it cannot be found, so execute it
    cache.register(child, extraChild)
    cache.execute(child) shouldBe true // its parent cannot be found, so execute it
    cache.register(extraChild, extraChild)
    cache.execute(extraChild) shouldBe true // it has an ancestor that cannot be found, so execute it
  }

  it should "execute if no status was found for the task" in {
    val root    = task
    val rootDef = Definition.buildRootDefinition(root)
    val lines   = Seq(rootDef).map(_.toString)
    val cache   = new SimpleTaskCache(lines)
    cache.register(root) // register the root task
    cache.register(root, root)
    cache.execute(root) shouldBe true
  }

  private trait GreedyResourcePicking extends UnitTask {
    override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
      val mem = Memory("1g")
      (8 to 1 by -1).map(c => ResourceSet(Cores(c), mem)).find(rs => availableResources.subset(rs).isDefined)
    }
  }

  private class MaybeFailTask(shouldFail: Boolean) extends SimpleInJvmTask with GreedyResourcePicking {
    def run(): Unit = if (shouldFail) throw new IllegalStateException("Failing") else Thread.sleep(1)
  }

  class SleepyPipeline(val numTasks: Int = 100, val failTaskIndex: Set[Int] = Set.empty, shouldFail: Boolean = false) extends Pipeline {
    private val dependencyProbability: Double = 0.1
    private val seed: Option[Long] = Some(42)
    private val toTask: Int => Task = i => new MaybeFailTask(shouldFail=failTaskIndex.contains(i)) withName s"task-$i"

    override def build(): Unit = {
      // create the tasks
      val tasks: Seq[Task] = for (i <- 0 to numTasks) yield toTask(i)

      // make them depend on previous tasks
      var rootTasks = Seq.range(start=0, numTasks).toSet
      val randomNumberGenerator = seed match {
        case Some(s) => new scala.util.Random(s)
        case None    => scala.util.Random
      }
      for (i <- 0 until numTasks) {
        for (j <- 0 until i) {
          if (randomNumberGenerator.nextFloat < dependencyProbability) {
            tasks(j) ==> tasks(i)
            rootTasks = rootTasks - i
          }
        }
      }

      rootTasks.foreach { i =>
        root ==> tasks(i)
      }
    }
  }

  def executeAndReplay(failTaskIndex: Set[Int]): Unit = {
    val tmpOut = Logger.out
    Logger.out = new PrintStream(new OutputStream {
      override def write(b: Int) = Unit
    })
    val replayLog = {
      val path = Files.createTempFile("replay.", ".log")
      path.toFile.deleteOnExit()
      path
    }

    // Execute the pipeline the first time.  If `failTaskIndex` has any values, those tasks will fail.
    val executor1 = Executor(experimentalExecution=true, resources=SystemResources.infinite)
    val pipeline1 = new SleepyPipeline(failTaskIndex=failTaskIndex, shouldFail=true)
    executor1.withReporter(new ExecutionLogger(replayLog))
    if (failTaskIndex.nonEmpty) executor1.execute(pipeline1) should be > 0
    else executor1.execute(pipeline1) shouldBe 0

    // Execute it a second time, with the replay log.  If `failTaskIndex` has any values, those tasks will not fail.
    val executor2 = Executor(experimentalExecution=true, resources=SystemResources.infinite)
    val pipeline2 = new SleepyPipeline(failTaskIndex=Set.empty, shouldFail=false)
    executor2.withReporter(TaskCache(replayLog))
    executor2.execute(pipeline2) shouldBe 0
    Logger.out = tmpOut
  }

  "SimpleTaskCache" should "replay a complicated workflow that succeeded all tasks" in {
    executeAndReplay(failTaskIndex=Set.empty)
  }

  it should "replay a complicated workflow whose first task failed" in {
    executeAndReplay(failTaskIndex=Set(0))
  }

  it should "replay a complicated workflow who has a single failing task somewhere in the middle" in {
    executeAndReplay(failTaskIndex=Set(50))
  }

  it should "replay a complicated workflow whose last task failed" in {
    executeAndReplay(failTaskIndex=Set(99))
  }

  it should "replay a complicated workflow where all tasks fail" in {
    executeAndReplay(failTaskIndex=Seq.range(0, 100).toSet)
  }

  it should "replay a complicated workflow where tasks fail" in {
    val tmpOut = Logger.out
    Logger.out = new PrintStream(new OutputStream {
      override def write(b: Int) = Unit
    })

    var reportLog = {
      val path = Files.createTempFile("report.", ".log")
      path.toFile.deleteOnExit()
      path
    }

    var replayLog = {
      val path = Files.createTempFile("replay.", ".log")
      path.toFile.deleteOnExit()
      path
    }

    // fail every task once!
    Seq.range(0, 100).foreach { failTaskIndex =>
      val executor = Executor(experimentalExecution=true, resources=SystemResources.infinite)
      val pipeline = new SleepyPipeline(failTaskIndex=Set(failTaskIndex), shouldFail=true)
      executor.withReporter(new ExecutionLogger(reportLog))
      if (0 < failTaskIndex) executor.withReporter(TaskCache(replayLog))
      executor.execute(pipeline) should be > 0
      // swap the replay and report log
      val tmp   = replayLog
      replayLog = reportLog
      reportLog = tmp
    }

    // run it one last time, without failing
    {
      val executor = Executor(experimentalExecution=true, resources=SystemResources.infinite)
      val pipeline = new SleepyPipeline(failTaskIndex=Set.empty, shouldFail=false)
      executor.withReporter(TaskCache(replayLog))
      executor.execute(pipeline) shouldBe 0
    }
    Logger.out = tmpOut
  }
}
