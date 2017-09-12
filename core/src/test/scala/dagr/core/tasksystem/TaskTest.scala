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

import java.nio.file.{Files, Path}
import java.time.Instant

import com.fulcrumgenomics.commons.util.LazyLogging
import dagr.api.DagrApi.TaskId
import dagr.api.models.util.{Cores, Memory, ResourceSet}
import dagr.core.UnitSpec
import dagr.core.execsystem._
import org.scalatest.OptionValues

import scala.collection.mutable.ListBuffer
import scala.io.Source

class TaskTest extends UnitSpec with LazyLogging with OptionValues {
  private val resources = ResourceSet(Cores(1), Memory("32M"))

  class SleepAndExitData(var sleepValue: Int, var exitCode: Int)

  class LazyTask(var sleepTime: Option[SleepAndExitData]) extends ProcessTask {
    name = "LazyTask"
    private val argBuffer = ListBuffer[Any]()
    override def args = argBuffer
    override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = Some(ResourceSet.empty)

    override def applyResources(resources: ResourceSet): Unit = {
      val sleepAndExitData: SleepAndExitData = sleepTime match {
        case Some(data) => data
        case _ => throw new RuntimeException("Could not get sleep and exit data")
      }
      argBuffer.append("sleep " + sleepAndExitData.sleepValue)
      argBuffer.append("exit " + sleepAndExitData.exitCode)
    }
  }

  class FirstTaskNeedsBuilding extends ProcessTask {
    name = "FirstTask"
    var sleepTime : Option[SleepAndExitData] = None
    override val args = List.empty
    override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = Some(ResourceSet.empty)
    override def applyResources(resources: ResourceSet): Unit = {
      sleepTime = Option(new SleepAndExitData(10, 0))
    }
  }

  class FirstTaskOnComplete extends ProcessTask with FixedResources {
    name = "FirstTask"
    var sleepTime: Option[SleepAndExitData] = None
    override val args = List.empty

    override def onComplete(exitCode: Int): Boolean = {
      sleepTime = Option(new SleepAndExitData(10, 0))
      true
    }
  }

  /**
   * Example #1: a "lazy task" that doesn't have all of its required inputs until
   * a previous task on which it is dependent has getTasks called.
   */
  {

    "Dagr" should "support a linker between two tasks set in getTasks" in {
      val firstTask = new FirstTaskNeedsBuilding()
      val lazyTask = new LazyTask(sleepTime = None)
      firstTask ==> Linker(firstTask, lazyTask){(f,l) => l.sleepTime = f.sleepTime} ==> lazyTask

      // so now lazyTask depends on the number from firstTask being not None
      firstTask.applyResources(ResourceSet.infinite)
      firstTask.sleepTime should be ('defined)
      firstTask.tasksDependingOnThisTask.foreach(t => t.asInstanceOf[InJvmTask].inJvmMethod())
      lazyTask.getTasks should have size 1
    }

    it should "fail if a linker creates a link in the wrong direction leading to a circular dependecy" in {
      val firstTask = new FirstTaskNeedsBuilding()
      val lazyTask = new LazyTask(sleepTime = firstTask.sleepTime)

      // Linker args are backwars - correct usage would be Linker(firstTask, lazyTask)
      firstTask ==> Linker(lazyTask, firstTask) { (l,f) => l.sleepTime = f.sleepTime } ==> lazyTask
      Task.hasCycle(firstTask) shouldBe true
    }

  /**
   * Example #2 a "lazy task" that doesn't have all of its required inputs until
   * a previous task on which it is dependent has onComplete called.
   */
    it should "support a task data connector between two tasks set in onComplete" in {
      val firstTask = new FirstTaskOnComplete()
      val lazyTask = new LazyTask(sleepTime = None)
      firstTask ==> Linker(firstTask, lazyTask){(f,l) => l.sleepTime = f.sleepTime} ==> lazyTask

      // so now lazyTask depends on the number from firstTask being not None
      firstTask.getTasks should have size 1
      firstTask.sleepTime should be (None)
      firstTask.onComplete(0) should be (true)
      firstTask.sleepTime should be ('defined)
      firstTask.tasksDependingOnThisTask.foreach(t => t.asInstanceOf[InJvmTask].inJvmMethod())
      lazyTask.getTasks should have size 1
    }

    it should "fail if a task data connector set in onComplete is not yet available" in {
      val firstTask = new FirstTaskOnComplete()
      val lazyTask = new LazyTask(sleepTime = firstTask.sleepTime)
      firstTask ==> Linker(firstTask, lazyTask){(f,l) => l.sleepTime = f.sleepTime} ==> lazyTask
      // So now lazyTask depends on the number from firstTask being not None.
      // If we call lazyTask.getTasks, it will call firstTask.getNumber, which will fail,
      // since fistTask.number is None.
      firstTask.getTasks should have size 1
      firstTask.sleepTime should be (None)
      firstTask.tasksDependingOnThisTask.foreach(t => t.asInstanceOf[InJvmTask].inJvmMethod())
      an[RuntimeException] should be thrownBy lazyTask.applyResources(ResourceSet.infinite)
    }
  }

  /**
   * Tests logging to a file.
   */
  {
    class TestProcessLogging extends ShellCommand("echo", "Testing") {
      name = "tests progress logging"
    }
    class TestInJvmLogging
      extends SimpleInJvmTask {
      name = "tests in-Jvm logging"
      def run(): Unit = logger.info("Testing")
    }

    def testTaskLogging(task: UnitTask, msg: String): Unit = {
      val script: Path = Files.createTempFile("script", ".sh")
      val logFile: Path = Files.createTempFile("log", ".log")
      // remember to run "getTasks" on the task
      task.getTasks should have size 1

      val taskRunner: TaskExecutionRunner = new TaskExecutionRunner
      val id: TaskId = 1

      // Run the task
      taskRunner.runTask(taskInfo = new TaskExecutionInfo(
          task           = task,
          initId         = id,
          script         = script,
          log            = logFile
        )
      )
      taskRunner.joinAll(1000)

      // Make sure the task completed
      val completedMap: Map[TaskId, (Int, Boolean)] = taskRunner.completedTasks()
      completedMap should contain key id
      completedMap.get(id).get._1 should be(0) // exit code
      completedMap.get(id).get._2 should be(true) // on complete

      // Read the log file and check that it contains the message
      val source: Source = Source.fromFile(logFile.toFile)
      val lines: String = try source.mkString finally source.close()
      lines should not be null
      lines.contains(msg) should be(true)
    }

    it should "support logging to a file for a Process task" in {
      testTaskLogging(task = new TestProcessLogging, msg = "Testing")
    }

    it should "support logging to a file for an inJvm task" in {
      testTaskLogging(task = new TestInJvmLogging, msg = "Testing")
    }
  }

  /** Tests the EitherTask class */
  {
    "EitherTask" should "be return the correct ask as the choice changes over time" in {
      val left  = new NoOpTask withName "leftTask"
      val right = new NoOpInJvmTask("rightTask")

      var choice : EitherTask.Choice = EitherTask.Left
      val eitherTask = EitherTask(left, right, choice)
      eitherTask.getTasks.head shouldBe left

      choice = EitherTask.Right
      eitherTask.getTasks.head shouldBe right
    }

    it should "run the left or right task depending on the flag with EitherTask" in {
      val left  = new NoOpTask withName "leftTask"
      val right = new NoOpInJvmTask("rightTask")
      EitherTask(left, right, EitherTask.Left).getTasks.head should be(left)
      EitherTask(left, right, EitherTask.Right).getTasks.head should be(right)

      EitherTask.of(left, right, goLeft=true).getTasks.head should be(left)
      EitherTask.of(left, right, goLeft=false).getTasks.head should be(right)
    }
  }

  class Noop(val n: String) extends NoOpTask() { override def toString: String = n }

  /** Tests the dependency DSL */
  {
    "Tasks" should "have the correct dependencies" in {
      val (a, b, c, d, e, f, g) = (new Noop("A"), new Noop("B"), new Noop("C"), new Noop("D"), new Noop("E"), new Noop("F"), new Noop("G"))
      a ==> (b :: c :: d) ==> e ==> (f :: g)

      a.tasksDependedOn.isEmpty shouldBe true
      a.tasksDependingOnThisTask should contain theSameElementsAs Traversable(b, c, d)

      b.tasksDependedOn should contain theSameElementsAs Traversable(a)
      b.tasksDependingOnThisTask should contain theSameElementsAs Traversable(e)

      c.tasksDependedOn should contain theSameElementsAs Traversable(a)
      c.tasksDependingOnThisTask should contain theSameElementsAs Traversable(e)

      d.tasksDependedOn should contain theSameElementsAs Traversable(a)
      d.tasksDependingOnThisTask should contain theSameElementsAs Traversable(e)

      e.tasksDependedOn should contain theSameElementsAs Traversable(b, c, d)
      e.tasksDependingOnThisTask should contain theSameElementsAs Traversable(f, g)

      f.tasksDependedOn should contain theSameElementsAs Traversable(e)
      f.tasksDependingOnThisTask.isEmpty shouldBe true

      g.tasksDependedOn should contain theSameElementsAs Traversable(e)
      g.tasksDependingOnThisTask.isEmpty shouldBe true

      a !=> (b :: c :: d)
      (b :: c :: d) !=> e
      e !=> (f :: g)

      List(a, b, c, d, e, f, g).foreach(t => t.tasksDependedOn.isEmpty shouldBe true)
      List(a, b, c, d, e, f, g).foreach(t => t.tasksDependingOnThisTask.isEmpty shouldBe true)
    }

    it should "bind :: grouping operations tighter than ==> and !=> dependency operations" in {
      val (a, b, c, d, e, f, g) = (new Noop("A"), new Noop("B"), new Noop("C"), new Noop("D"), new Noop("E"), new Noop("F"), new Noop("G"))
      a :: b ==> c :: d ==> e :: f :: g

      a.tasksDependedOn.isEmpty shouldBe true
      a.tasksDependingOnThisTask should contain theSameElementsAs Traversable(c, d)

      b.tasksDependedOn.isEmpty shouldBe true
      b.tasksDependingOnThisTask should contain theSameElementsAs Traversable(c, d)

      c.tasksDependedOn should contain theSameElementsAs Traversable(a, b)
      c.tasksDependingOnThisTask should contain theSameElementsAs Traversable(e, f, g)

      d.tasksDependedOn should contain theSameElementsAs Traversable(a, b)
      d.tasksDependingOnThisTask should contain theSameElementsAs Traversable(e, f, g)

      e.tasksDependedOn should contain theSameElementsAs Traversable(c, d)
      e.tasksDependingOnThisTask.isEmpty shouldBe true

      f.tasksDependedOn should contain theSameElementsAs Traversable(c, d)
      f.tasksDependingOnThisTask.isEmpty shouldBe true

      g.tasksDependedOn should contain theSameElementsAs Traversable(c, d)
      g.tasksDependingOnThisTask.isEmpty shouldBe true

      a :: b !=> c :: d
      c :: d !=> e :: f :: g

      List(a, b, c, d, e, f, g).foreach(t => t.tasksDependedOn.isEmpty shouldBe true)
      List(a, b, c, d, e, f, g).foreach(t => t.tasksDependingOnThisTask.isEmpty shouldBe true)
    }
  }

  /** Tests cycle detection in a DAG */
  {
    "Task" should "not detect a cycle with a single task" in {
      Task.hasCycle(new NoOpTask()) should be(false)
    }

    it should "detect a cycle with a task dependent on itself" in {
      val task: Task = new NoOpTask()
      task ==> task
      Task.hasCycle(task) should be(true)
    }

    it should "not detect a cycle with a two tasks and a single dependency" in {
      val firstTask: Task = new NoOpTask()
      val secondTask: Task = new NoOpTask()
      firstTask ==> secondTask
      List(firstTask, secondTask).foreach(t => Task.hasCycle(t) should be(false))
    }

    it should "detect a cycle with a two tasks and a cyclical dependency" in {
      val firstTask: Task = new NoOpTask()
      val secondTask: Task = new NoOpTask()
      firstTask ==> secondTask ==> firstTask
      List(firstTask, secondTask).foreach(t => Task.hasCycle(t) should be(true))
    }

    it should "not detect a cycle with a three tasks and a linear dependency graph" in {
      val firstTask: Task = new NoOpTask()
      val secondTask: Task = new NoOpTask()
      val thirdTask: Task = new NoOpTask()
      firstTask ==> secondTask ==> thirdTask
      List(firstTask, secondTask, thirdTask).foreach(t => Task.hasCycle(t) should be(false))
    }

    it should "detect a cycle with a three tasks and a cyclical dependency graph" in {
      val firstTask: Task = new NoOpTask()
      val secondTask: Task = new NoOpTask()
      val thirdTask: Task = new NoOpTask()
      firstTask ==> secondTask ==> thirdTask ==> firstTask
      List(firstTask, secondTask, thirdTask).foreach(t => Task.hasCycle(t) should be(true))
    }

    it should "not detect a cycle with a three tasks with one predecessor and two successors" in {
      val firstTask: Task = new NoOpTask()
      val secondTask: Task = new NoOpTask()
      val thirdTask: Task = new NoOpTask()
      firstTask ==> (secondTask :: thirdTask)
      List(firstTask, secondTask, thirdTask).foreach(t => Task.hasCycle(t) should be(false))
    }

    it should "not detect a cycle with a three tasks with one predecessor and two successors, with one successor depending on the other" in {
      val firstTask: Task = new NoOpTask()
      val secondTask: Task = new NoOpTask()
      val thirdTask: Task = new NoOpTask()
      firstTask ==> (secondTask :: thirdTask)
      secondTask ==> thirdTask
      List(firstTask, secondTask, thirdTask).foreach(t => Task.hasCycle(t) should be(false))
    }

    it should "detect a cycle in a 'binary' tree with a cycle on the left path" in {
      val root: Task = new NoOpTask() withName "root"
      val leftMid: Task = new NoOpTask() withName "leftMid"
      val rightMid: Task = new NoOpTask() withName "rightMid"
      val cycle: Seq[Task] = for (i <- 1 to 4) yield new NoOpTask() withName s"cycle-$i"
      root ==> (leftMid :: rightMid)
      // create the cycle of dependencies
      for (i <- 1 to 4) cycle(i-1) ==> cycle((i+2) % 4)
      // cycle in cycles, but no cycles in root/leftMid/rightMid
      cycle.foreach(t => Task.hasCycle(t) should be(true))
      //List(root, leftMid, rightMid).foreach(t => Task.hasCycle(t) should be(false))
      // now hook up the cycle to leftMid
      leftMid ==> cycle.head
      // test them again
      (List(root, leftMid, rightMid) ::: cycle.toList).foreach(t => Task.hasCycle(t) should be(true))
      // unhook it
      leftMid !=> cycle.head
      // now hook up the leftMid to cycle
      cycle.head ==> leftMid
      // test them again
      (List(root, leftMid, rightMid) ::: cycle.toList).foreach(t => Task.hasCycle(t) should be(true))
    }
    // TODO: two roots into a graph
  }

  // Tests that SimpleJvmTasks behave as expected
  {
    "SimpleInJvmTask" should "execute as expected" in {
      val b = ListBuffer[Int]()
      val s = SimpleInJvmTask("foo", b += 7)
      val s2 = SimpleInJvmTask { b(0) -= 7 }
      val s3 = SimpleInJvmTask { throw new IllegalArgumentException("Expected Exception") }

      b.length shouldBe 0

      s.inJvmMethod() shouldBe 0
      b.length shouldBe 1
      b.head shouldBe 7

      s2.inJvmMethod() shouldBe 0
      b.length shouldBe 1
      b.head shouldBe 0

      s3.inJvmMethod() shouldBe 1
    }
  }

  {
    case class P(xs: String*) extends ProcessTask with FixedResources {
      override def args = xs.toSeq
    }

    "ProcessTask" should "not escape arguments that don't need it" in {
      P("foo", "bar").commandLine shouldBe "foo bar"
      P("-f", "--bar", "--XX:whee").commandLine shouldBe "-f --bar --XX:whee"
      P("/foo/bar", "a=b", "c-=d").commandLine shouldBe "/foo/bar a=b c-=d"
    }

    it should "quote arguments containing spaces and other special characters in arguments" in {
      P("foo", "oopsy daisy").commandLine shouldBe "foo 'oopsy daisy'"
      P("rm", "*/*").commandLine shouldBe "rm '*/*'"
      P("ok", "not ok", "ok", "not;ok", "ok", "$notok", "ok").commandLine shouldBe "ok 'not ok' ok 'not;ok' ok '$notok' ok"
    }

    it should "escape single quotes in arguments that don't otherwise need quoting" in {
      P("foo", "my'thing", "is'", "silly'''").commandLine shouldBe """foo my\'thing is\' silly\'\'\'"""
    }

    it should "quote all special character in arguments that have single quotes" in {
      P("it's cold outside", "what?!", "I'm a *").commandLine shouldBe """it\'s\ cold\ outside 'what?!' I\'m\ a\ \*"""
    }
  }

  private class TestingInfo(task: Task, initStatus: TaskStatus) extends Task.TaskInfo(task=task, initStatus=initStatus) {
    override protected[core] def startDate = None
    override protected[core] def submissionDate = None
    override protected[core] def endDate = None
  }

  "TaskInfo.update" should "only add a time point if the status changes" in {
    val task = new NoOpInJvmTask("no-op")
    val info = new TestingInfo(task=task, initStatus=TaskStatus.Unknown)

    info.timePoints.size shouldBe 1
    val instant = info.get(TaskStatus.Unknown).value

    info(TaskStatus.Unknown) = Instant.now()
    info.timePoints.size shouldBe 1
    info.get(TaskStatus.Unknown).value shouldBe instant

    val startInstant = Instant.now()
    info(TaskStatus.Started) = startInstant
    info.timePoints.size shouldBe 2
    info.get(TaskStatus.Unknown).value shouldBe instant
    info.get(TaskStatus.Started).value shouldBe startInstant
  }

  "TaskInfo.latestStatus" should "get the latest instant of the given type of status" in {
    val task = new NoOpInJvmTask("no-op")
    val info = new TestingInfo(task=task, initStatus=TaskStatus.Unknown)

    info.latestStatus[TaskStatus.Unknown.type].value shouldBe info.get(TaskStatus.Unknown).value

    val startInstant = Instant.now()
    info(TaskStatus.Started) = startInstant
    info.latestStatus[TaskStatus.Started.type].value shouldBe startInstant

    val failedInstant = Instant.now()
    info(TaskStatus.FailedExecution) = failedInstant
    info.latestStatus[TaskStatus.FailedExecution.type].value shouldBe failedInstant

    val succeededInstant = Instant.now()
    info(TaskStatus.SucceededExecution) = succeededInstant
    info.latestStatus[TaskStatus.SucceededExecution.type].value shouldBe succeededInstant

    info.latestStatus[TaskStatus.Completed].value shouldBe succeededInstant
  }

  "TaskInfo.statusTime" should "get the instant of latest status" in {
    val task = new NoOpInJvmTask("no-op")
    val info = new TestingInfo(task=task, initStatus=TaskStatus.Unknown)

    info.statusTime shouldBe info.get(TaskStatus.Unknown).value

    val startInstant = Instant.now()
    info(TaskStatus.Started) = startInstant
    info.statusTime shouldBe startInstant
  }

  "Task.execsystemTaskInfo" should "throw an exception if not the correct type" in {
    val task = new NoOpInJvmTask("no-op")
    new TestingInfo(task=task, initStatus=TaskStatus.Unknown)

    an[IllegalStateException] should be thrownBy task.execsystemTaskInfo
  }

  "Task.removeDependency" should "return false if no dependency existed" in {
    val left = new NoOpInJvmTask("no-op")
    val right = new NoOpInJvmTask("no-op")
    // no dependency
    left.removeDependency(right) shouldBe false

    // right is a dependency on left
    left ==> right
    right.removeDependency(left) shouldBe true

    // left is not a dependency on right
    left ==> right
    left.removeDependency(right) shouldBe false
    right.removeDependency(right) shouldBe false
  }
}
