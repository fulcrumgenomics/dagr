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

import dagr.core.DagrDef._
import com.fulcrumgenomics.commons.util.{LazyLogging, LogLevel, Logger}
import dagr.core.tasksystem._
import com.fulcrumgenomics.commons.collection._
import dagr.core.{TestTags, UnitSpec}
import org.scalatest._

import scala.collection.mutable.ListBuffer

object TaskManagerTest {

  trait TryThreeTimesTask extends MultipleRetry {
    override def maxNumIterations: Int = 3
  }

  class SucceedOnTheThirdTry extends TryThreeTimesTask with ProcessTask with FixedResources {
    var args: Seq[Any] = Seq("exit", "1")
    override def retry(resources: SystemResources, taskInfo: TaskExecutionInfo): Boolean = {
      if (maxNumIterations <= taskInfo.attemptIndex) this.args = Seq("exit", "0")
      true
    }
  }
}

trait TestTaskManager extends TaskManager {
  override def joinOnRunningTasks(millis: Long) = super.joinOnRunningTasks(millis)
}

class TaskManagerTest extends UnitSpec with OptionValues with LazyLogging with BeforeAndAfterAll {

  override def beforeAll(): Unit = Logger.level = LogLevel.Fatal
  override def afterAll(): Unit = Logger.level = LogLevel.Info

  def getDefaultTaskManager(sleepMilliseconds: Int = 10): TestTaskManager = new TaskManager(
    taskManagerResources = SystemResources.infinite,
    scriptsDirectory = None,
    sleepMilliseconds = sleepMilliseconds
  ) with TestTaskManager

  private def runSchedulerOnce(taskManager: TestTaskManager,
                               tasksToScheduleContains: List[Task],
                               runningTasksContains: List[Task],
                               completedTasksContains: List[Task],
                               failedAreCompleted: Boolean = true) = {
    taskManager.joinOnRunningTasks(1000)
    val (readyTasks, tasksToSchedule, runningTasks, completedTasks) = taskManager.stepExecution()
    tasksToScheduleContains.foreach(task => tasksToSchedule should contain(task))
    runningTasksContains.foreach(task => runningTasks should contain(task))
    completedTasksContains.foreach(task => completedTasks should contain(task))
  }

  // Expects that the task will complete (either successfully or with failure) on the nth try
  def tryTaskNTimes(taskManager: TestTaskManager, task: UnitTask, numTimes: Int, taskIsDoneFinally: Boolean, failedAreCompletedFinally: Boolean = true) = {

    // add the task
    taskManager.addTask(task)
    TaskStatus.isTaskDone(taskManager.taskStatusFor(task).get) should be(false)

    // run the task, and do not report failed tasks as completed
    for (i <- 1 to numTimes) {
      logger.debug("running the scheduler the " + i + "th time")
      runSchedulerOnce(taskManager = taskManager, tasksToScheduleContains = List[Task](task), runningTasksContains = Nil, completedTasksContains = Nil, failedAreCompleted = false)
      TaskStatus.isTaskDone(taskStatus = taskManager.taskStatusFor(task).get, failedIsDone = false) should be(false)
    }

    // run the task a fourth time, but set that any failed tasks should be assumed to be completed
    logger.debug("running the scheduler the last (" + (numTimes + 1) + "th) time")
    runSchedulerOnce(taskManager = taskManager, tasksToScheduleContains = Nil, runningTasksContains = Nil, completedTasksContains = List[Task](task), failedAreCompleted = failedAreCompletedFinally)
    TaskStatus.isTaskDone(taskStatus = taskManager.taskStatusFor(task).get, failedIsDone = failedAreCompletedFinally) should be(taskIsDoneFinally)
  }

  "TaskManager" should "not overwrite an existing task when adding a task, or throw an IllegalArgumentException when ignoreExists is false" in {
    val task: UnitTask = new ShellCommand("exit", "0") withName "exit 0" requires ResourceSet.empty
    val taskManager: TestTaskManager = getDefaultTaskManager()
    taskManager.addTasks(tasks=Seq(task, task), ignoreExists=true) shouldBe List(0, 0)
    an[IllegalArgumentException] should be thrownBy taskManager.addTask(task=task, enclosingNode=None, ignoreExists=false)
  }

  it should "get the task status for only tracked tasks" in {
    val task: UnitTask = new ShellCommand("exit", "0") withName "exit 0" requires ResourceSet.empty
    val taskManager: TestTaskManager = getDefaultTaskManager()
    taskManager.addTask(task=task) shouldBe 0
    taskManager.taskStatusFor(0) shouldBe 'defined
    taskManager.taskStatusFor(1) shouldBe 'empty
  }

  it should "get the graph node state for only tracked tasks" in {
    val task: UnitTask = new ShellCommand("exit", "0") withName "exit 0" requires ResourceSet.empty
    val taskManager: TestTaskManager = getDefaultTaskManager()
    taskManager.addTask(task=task) shouldBe 0
    taskManager.graphNodeStateFor(0) shouldBe 'defined
    taskManager.graphNodeStateFor(1) shouldBe 'empty
  }

  // ******************************************
  // Simple Tasks
  // ******************************************

  it should "run a simple task" in {
    val task: UnitTask = new ShellCommand("exit", "0") withName "exit 0" requires ResourceSet.empty
    val taskManager: TestTaskManager = getDefaultTaskManager()

    tryTaskNTimes(taskManager = taskManager, task = task, numTimes = 1, taskIsDoneFinally = true, failedAreCompletedFinally = true)
  }

  it should "run a simple task that fails but we allow it" in {
    val task: UnitTask = new ShellCommand("exit", "1") withName "exit 1"
    val taskManager: TestTaskManager = getDefaultTaskManager()

    tryTaskNTimes(taskManager = taskManager, task = task, numTimes = 1, taskIsDoneFinally = true, failedAreCompletedFinally = true)
  }

  it should "run a simple task that fails and is never done" in {
    val task: UnitTask = new ShellCommand("exit", "1") withName "exit 1"
    val taskManager: TestTaskManager = getDefaultTaskManager()

    tryTaskNTimes(taskManager = taskManager, task = task, numTimes = 1, taskIsDoneFinally = false, failedAreCompletedFinally = false)
  }

  it should "retry a task three times that ends up failed" in {
    val task: UnitTask = new ShellCommand("exit 1") with TaskManagerTest.TryThreeTimesTask withName "retry three times"
    val taskManager: TestTaskManager = getDefaultTaskManager()

    // try it four times, and it will keep failing, and not omit a task at the last attempt
    tryTaskNTimes(taskManager = taskManager, task = task, numTimes = 4, taskIsDoneFinally = true, failedAreCompletedFinally = true)
  }

  it should "retry a task three times and succeed on the last try" in {
    val task: UnitTask = new TaskManagerTest.SucceedOnTheThirdTry withName "succeed on the third try"
    val taskManager: TestTaskManager = getDefaultTaskManager()

    // try it three times, and it will keep failing, and not omit a task at the last attempt
    tryTaskNTimes(taskManager = taskManager, task = task, numTimes = 3, taskIsDoneFinally = false, failedAreCompletedFinally = true)
  }

  def runSimpleEndToEnd(task: UnitTask = new ShellCommand("exit", "0") withName "exit 0", simulate: Boolean): Unit = {
    val map: BiMap[Task, TaskExecutionInfo] = TaskManager.run(
      task                 = task,
      sleepMilliseconds    = 10,
      taskManagerResources = Some(SystemResources.infinite),
      scriptsDirectory     = None,
      simulate             = simulate,
      failFast             = true)

    map.containsKey(task) should be(true)

    val taskInfo: TaskExecutionInfo = map.valueFor(task).get
    taskInfo.taskId should be(0)
    taskInfo.task should be(task)
    taskInfo.status should be(TaskStatus.SUCCEEDED)
    taskInfo.attemptIndex should be(1)
  }

  it should "run a simple task end-to-end" in {
    runSimpleEndToEnd(simulate = false)
  }

  it should "run a simple task end-to-end in simulate mode" in {
    runSimpleEndToEnd(simulate = true)
  }

  it should "run a simple task that fails but we allow in simulate mode" in {
    val task: UnitTask = new ShellCommand("exit", "1") withName "exit 1"
    runSimpleEndToEnd(task = task, simulate = true)
  }

  it should "terminate all running jobs before returning from runAllTasks" in {
    val longTask: UnitTask = new ShellCommand("sleep", "1000") withName "sleep 1000"
    val failedTask: UnitTask = new ShellCommand("exit", "1") withName "exit 1"

    val taskManager: TestTaskManager = getDefaultTaskManager(sleepMilliseconds=1)
    taskManager.addTasks(longTask, failedTask)
    taskManager.runToCompletion(failFast=true)
    taskManager.taskStatusFor(failedTask).value should be(TaskStatus.FAILED_COMMAND)
    taskManager.graphNodeStateFor(failedTask).value should be(GraphNodeState.COMPLETED)
    taskManager.taskStatusFor(longTask).value should be(TaskStatus.STOPPED)
  }

  it should "not schedule and run tasks that have failed dependencies" in {
    val List(a, b, c) = List(0,1,0).map(c => new ShellCommand("exit", c.toString))
    a ==> b ==> c
    val tm = getDefaultTaskManager(sleepMilliseconds=1)
    tm.addTasks(a, b, c)
    tm.runToCompletion(failFast=false)
    tm.taskStatusFor(a).value shouldBe TaskStatus.SUCCEEDED
    tm.graphNodeFor(a).value.state shouldBe GraphNodeState.COMPLETED
    tm.taskStatusFor(b).value shouldBe TaskStatus.FAILED_COMMAND
    tm.graphNodeFor(b).value.state shouldBe GraphNodeState.COMPLETED
    tm.taskStatusFor(c).value shouldBe TaskStatus.UNKNOWN
    tm.graphNodeFor(c).value.state shouldBe GraphNodeState.PREDECESSORS_AND_UNEXPANDED
  }

  it should "not schedule and run tasks that have failed dependencies and complete all when failed tasks are manually succeeded" in {
    val List(a, b, c) = List(0,1,0).map(c => new ShellCommand("exit", c.toString))
    a ==> b ==> c
    val tm = getDefaultTaskManager(sleepMilliseconds=1)
    tm.addTasks(a, b, c)
    tm.runToCompletion(failFast=false)
    tm.taskStatusFor(a).value shouldBe TaskStatus.SUCCEEDED
    tm.graphNodeFor(a).value.state shouldBe GraphNodeState.COMPLETED
    tm.taskStatusFor(b).value shouldBe TaskStatus.FAILED_COMMAND
    tm.graphNodeFor(b).value.state shouldBe GraphNodeState.COMPLETED
    tm.taskStatusFor(c).value shouldBe TaskStatus.UNKNOWN
    tm.graphNodeFor(c).value.state shouldBe GraphNodeState.PREDECESSORS_AND_UNEXPANDED

    // manually succeed b
    tm.taskExecutionInfoFor(b).foreach(_.status = TaskStatus.MANUALLY_SUCCEEDED)
    tm.runToCompletion(failFast=false)
    tm.taskStatusFor(a).value shouldBe TaskStatus.SUCCEEDED
    tm.graphNodeFor(a).value.state shouldBe GraphNodeState.COMPLETED
    tm.taskStatusFor(b).value shouldBe TaskStatus.MANUALLY_SUCCEEDED
    tm.graphNodeFor(b).value.state shouldBe GraphNodeState.COMPLETED
    tm.taskStatusFor(c).value shouldBe TaskStatus.SUCCEEDED
    tm.graphNodeFor(c).value.state   shouldBe GraphNodeState.COMPLETED
  }

  // ******************************************
  // Task retry and replacement
  // *****************************************
  protected class TestPipeline(original: Task) extends Pipeline {
    withName("TestWorkflow")
    override def build(): Unit = {
      val first: UnitTask = new ShellCommand("exit", "0") withName "first exit 0"
      val third: UnitTask = new ShellCommand("exit", "0") withName "third exit 0"
      // dependencies: first <- original <- third
      root ==> first ==> original ==> third
    }
  }

  def doTryAgainRun(original: Task,
                    originalTaskStatus: TaskStatus.Value,
                    originalGraphNodeState: GraphNodeState.Value,
                    taskManager: TestTaskManager): Unit = {
    // run it once, make sure tasks that are failed are not marked as completed
    taskManager.runToCompletion(failFast=true)

    // the task identifier for 'original' should now be found
    val originalTaskId: TaskId = taskManager.taskFor(original).value

    // check the original task status
    val taskInfo: TaskExecutionInfo = taskManager.taskExecutionInfoFor(originalTaskId).value
    taskInfo.status should be(originalTaskStatus)

    // check the original graph node state
    val state: GraphNodeState.Value = taskManager.graphNodeStateFor(originalTaskId).value
    state should be(originalGraphNodeState)
  }

  def doTryAgain(original: Task,
                 replacement: Task,
                 replaceTask: Boolean = true,
                 taskManager: TestTaskManager) {

    val (originalTaskStatus, originalGraphNodeState) = replaceTask match {
      case true => (TaskStatus.UNKNOWN, GraphNodeState.NO_PREDECESSORS)
      case false => (TaskStatus.FAILED_COMMAND, GraphNodeState.COMPLETED)
    }

    val wf: TestPipeline = new TestPipeline(original)
    taskManager.addTask(wf) should be(0)

    // run and check
    doTryAgainRun(original = original, originalTaskStatus = originalTaskStatus, originalGraphNodeState = originalGraphNodeState, taskManager = taskManager)

    if (replaceTask) {
      // replace
      logger.debug("*** REPLACING TASK ***")
      taskManager.replaceTask(original = original, replacement = replacement)
      original._taskInfo shouldBe 'empty
    }
    else {
      // resubmit...
      throw new IllegalStateException("Resubmission has been removed")
      //logger.debug("*** RESUBMIT TASK ***")
      //taskManager.resubmitTask(original) shouldBe true
    }

    // run and check again
    doTryAgainRun(original = if (replaceTask) replacement else original,
                  originalTaskStatus = TaskStatus.SUCCEEDED,
                  originalGraphNodeState = GraphNodeState.COMPLETED,
                  taskManager = taskManager)
  }

  it should "replace a task that could not be scheduled due to OOM and re-run with less memory to completion" in {
    val original = new ShellCommand("exit", "0").requires(memory=Memory("2G")) withName "Too much memory"
    val replacement = new ShellCommand("exit", "0").requires(memory=Memory("1G")) withName "Just enough memory"
    val taskManager: TestTaskManager = new TaskManager(taskManagerResources = new SystemResources(Cores(1), Memory("1G"), Memory(0)), scriptsDirectory = None) with TestTaskManager

    // just in case
    original.resources.memory.bytes should be(Resource.parseSizeToBytes("2G"))
    replacement.resources.memory.bytes should be(Resource.parseSizeToBytes("1G"))
    taskManager.getTaskManagerResources.systemMemory.value should be(Resource.parseSizeToBytes("1G"))

    doTryAgain(original = original, replacement = replacement, replaceTask = true, taskManager = taskManager)
  }

  // Really simple class that when retry gets called, it changes its args, but doesn't asked to be retried!
  class RetryMutatorTask extends Retry with ProcessTask with FixedResources {
    private var fail = true
    override def args = List("exit", if (fail) "1"  else "0")
    override def retry(resources: SystemResources, taskInfo: TaskExecutionInfo): Boolean = {
      fail = false
      false
    }
  }

  it should "handle two tasks with the same hashcode/equals" in {
    val first = new SimpleInJvmTask(0) {
      override def hashCode(): Int = 0
      override def equals(obj: Any): Boolean = this.hashCode() == obj.hashCode()
    }
    val second = new SimpleInJvmTask(0) {
      override def hashCode(): Int = 0
      override def equals(obj: Any): Boolean = this.hashCode() == obj.hashCode()
    }
    val taskManager: TestTaskManager = getDefaultTaskManager()
    taskManager.addTask(first)
    taskManager.graphNodeFor(first).nonEmpty shouldBe true
    taskManager.graphNodeFor(second).isEmpty shouldBe true
  }

  // This test fails occasionally, so there likely is a race condition.  Turning it off until `resubmit` is used.
  /*
  it should "resubmit a task that failed and re-run to completion" in {
    val original: UnitTask = new RetryMutatorTask
    val taskManager: TestTaskManager = getDefaultTaskManager()

    doTryAgain(original = original, replacement = null, replaceTask = false, taskManager = taskManager)
  }
  */

  it should "return false when replacing an un-tracked task" in {
    val task: UnitTask = new ShellCommand("exit", "0") withName "Blah"
    val taskManager: TestTaskManager = getDefaultTaskManager()
    taskManager.replaceTask(task, null) should be(false)
  }

  // This test fails occasionally, so there likely is a race condition.  Turning it off until `resubmit` is used.
  /*
  it should "return false when resubmitting an un-tracked task" in {
    val task: UnitTask = new ShellCommand("exit 0") withName "Blah"
    val taskManager: TestTaskManager = getDefaultTaskManager()
    taskManager.resubmitTask(task) should be(false)
  }
  */

  // ******************************************
  // onComplete success and failure
  // ******************************************

  // the onComplete method returns false until the retry method is called
  private trait FailOnCompleteTask extends Retry with UnitTask {
    private var onCompleteValue = false

    override def onComplete(exitCode: Int): Boolean = onCompleteValue

    override def retry(resources: SystemResources, taskInfo: TaskExecutionInfo): Boolean = {
      onCompleteValue = true
      true
    }
  }

  // the onComplete method modifies the arguments for this task, and in its first call is false, otherwise true
  private class FailOnCompleteAndClearArgsTask(name: String, originalArgs: List[String], newArgs: List[String] = List("exit", "0"))
    extends Retry with ProcessTask with FixedResources {
    withName(name)
    requires(ResourceSet.empty)
    private var onCompleteValue = false
    override def args = if (!onCompleteValue) originalArgs else newArgs

    override def onComplete(exitCode: Int): Boolean = {
      val previousOnCompleteValue: Boolean = onCompleteValue
      onCompleteValue = true
      previousOnCompleteValue
    }

    override def retry(resources: SystemResources, taskInfo: TaskExecutionInfo): Boolean = true
  }

  // The first time this task is run, it has exit code 1 and fails onComplete.
  // The second time this task is run, it has exit code 0 and fails onComplete
  // The third time and subsequent time this task is run, it has exit code 0 and succeed onComplete.
  private class MultiFailTask(name: String) extends Retry with ProcessTask with FixedResources {
    withName(name)
    private var onCompleteValue = false
    private var attemptIndex = 0
    override def args = if (attemptIndex == 0) List("exit", "1") else List("exit", "0")


    override def onComplete(exitCode: Int): Boolean = {
      val previousOnCompleteValue: Boolean = onCompleteValue
      // first attempt
      if (1 == attemptIndex) {
        onCompleteValue = true
      }
      previousOnCompleteValue
    }

    override def retry(resources: SystemResources, taskInfo: TaskExecutionInfo): Boolean = {
      attemptIndex = taskInfo.attemptIndex
      true
    }
  }

  def runTasksMultipleTimes(taskManager: TestTaskManager, task: UnitTask, statuses: List[(TaskStatus.Value, Int, Boolean)]): Unit = {
    // add the task
    taskManager.addTask(task)
    TaskStatus.isTaskDone(taskManager.taskStatusFor(task).get) should be(false)
    val taskId: TaskId = taskManager.taskFor(task).value

    for ((taskStatus, exitCode, onCompleteSuccessful) <- statuses) {
      // run the task once
      runSchedulerOnce(taskManager = taskManager, tasksToScheduleContains = List[Task](task), runningTasksContains = Nil, completedTasksContains = Nil, failedAreCompleted = false)
      taskManager.joinOnRunningTasks(2000)

      // we need to check the status of the task after it has completed but before it has been retried, so do this part manually.
      val completedTasks = taskManager.completedTasks(failedAreCompleted = false) // get the complete tasks from the task runner

      // now we can check status, exit code, and onComplete success
      completedTasks.get(taskId).value._1 should be(exitCode)
      completedTasks.get(taskId).value._2 should be(onCompleteSuccessful)
      taskManager.taskStatusFor(task).value should be(taskStatus)

      // retry and update the completed tasks
      completedTasks.keys.foreach(tid => {
        taskManager.processCompletedTask(tid, true)
      })
    }
  }

  it should "run a task that fails its onComplete method, is retried, where it modifies the onComplete method return value, and succeeds" in {
    val task: UnitTask = new ShellCommand("exit", "0") with FailOnCompleteTask withName "Dummy"
    val taskManager: TestTaskManager = getDefaultTaskManager()
    runTasksMultipleTimes(taskManager = taskManager, task = task, statuses = List((TaskStatus.FAILED_ON_COMPLETE, 0, false), (TaskStatus.SUCCEEDED, 0, true)))
  }

  it should "run a task that fails its onComplete method, whereby it changes its args to empty, and succeeds" in {
    val task: UnitTask = new FailOnCompleteAndClearArgsTask(name = "Dummy", originalArgs = List("exit", "0"))
    val taskManager: TestTaskManager = getDefaultTaskManager()
    runTasksMultipleTimes(taskManager = taskManager, task = task, statuses = List((TaskStatus.FAILED_ON_COMPLETE, 0, false), (TaskStatus.SUCCEEDED, 0, true)))
  }

  it should "run a task, that its onComplete method mutates its args and return value based on the attempt index" in {
    val task: UnitTask = new MultiFailTask(name = "Dummy")
    val taskManager: TestTaskManager = getDefaultTaskManager()

    runTasksMultipleTimes(taskManager = taskManager,
      task = task,
      statuses = List(
        (TaskStatus.FAILED_COMMAND, 1, false),
        (TaskStatus.FAILED_ON_COMPLETE, 0, false),
        (TaskStatus.SUCCEEDED, 0, true)
      )
    )
  }

  // ********************************************
  // in Jvm tasks
  // ********************************************

  private class SimpleInJvmTask(exitCode: Int) extends InJvmTask with FixedResources {
    override def inJvmMethod(): Int = exitCode
  }

  private class SimpleRetryInJvmTask extends InJvmTask with Retry with FixedResources {
    private var exitCode = 1

    override def inJvmMethod(): Int = exitCode

    override def retry(resources: SystemResources, taskInfo: TaskExecutionInfo): Boolean = {
      exitCode = 0
      true
    }
  }

  private class SimpleRetryOnCompleteInJvmTask extends InJvmTask with Retry with FixedResources {
    private var exitCode = 1

    override def inJvmMethod(): Int = exitCode

    override def retry(resources: SystemResources, taskInfo: TaskExecutionInfo): Boolean = {
      true
    }

    override def onComplete(exitCode: Int): Boolean = {
      this.exitCode = 0
      true
    }
  }

  // The first time this task is run, it has exit code 1 and fails onComplete.
  // The second time this task is run, it has exit code 0 and fails onComplete
  // The third time and subsequent time this task is run, it has exit code 0 and succeed onComplete.
  private class MultiFailInJvmTask(name: String) extends InJvmTask with Retry with FixedResources {
    private var onCompleteValue = false
    private var attemptIndex = 0
    private var exitCode = 1

    override def inJvmMethod(): Int = exitCode

    override def onComplete(exitCode: Int): Boolean = {
      val previousOnCompleteValue: Boolean = onCompleteValue
      // first attempt
      if (0 == attemptIndex) {
        // the first call to onComplete
        this.exitCode = 0
      }
      else if (1 == attemptIndex) {
        onCompleteValue = true
      }
      previousOnCompleteValue
    }

    override def retry(resources: SystemResources, taskInfo: TaskExecutionInfo): Boolean = {
      attemptIndex = taskInfo.attemptIndex
      true
    }
  }

  it should "run an in Jvm task that succeeds" in {
    runSimpleEndToEnd(task = new SimpleInJvmTask(exitCode = 0), simulate = false)
  }

  it should "run an in Jvm that fails" in {
    val taskManager: TestTaskManager = getDefaultTaskManager()
    tryTaskNTimes(taskManager = taskManager, task = new SimpleInJvmTask(exitCode = 1), numTimes = 1, taskIsDoneFinally = true, failedAreCompletedFinally = true)
  }

  it should "run inJvm tasks that can retry" in {
    val taskManager: TestTaskManager = getDefaultTaskManager()
    tryTaskNTimes(taskManager = taskManager, task = new SimpleRetryInJvmTask, numTimes = 2, taskIsDoneFinally = true, failedAreCompletedFinally = true)
    tryTaskNTimes(taskManager = taskManager, task = new SimpleRetryOnCompleteInJvmTask, numTimes = 2, taskIsDoneFinally = true, failedAreCompletedFinally = true)
  }

  it should "run an in Jvm task, that its onComplete method mutates its args and return value based on the attempt index" in {
    val task: UnitTask = new MultiFailInJvmTask(name = "Dummy")
    val taskManager: TestTaskManager = getDefaultTaskManager()

    runTasksMultipleTimes(taskManager = taskManager,
      task = task,
      statuses = List(
        (TaskStatus.FAILED_COMMAND, 1, false),
        (TaskStatus.FAILED_ON_COMPLETE, 0, false),
        (TaskStatus.SUCCEEDED, 0, true)
      )
    )
  }

  // *************************************************
  // Orphans: tasks whose predecessors have not been added
  // *************************************************

  it should "run a task for which its predecessor has not been added and put it into an orphan state" in {
    val predecessor: UnitTask = new ShellCommand("exit", "0") withName "predecessor"
    val successor: UnitTask = new ShellCommand("exit", "0") withName "successor"
    val taskManager: TestTaskManager = getDefaultTaskManager()
    predecessor ==> successor
    taskManager.addTask(successor)
    taskManager.graphNodeStateFor(successor).value should be(GraphNodeState.ORPHAN)
    taskManager.stepExecution()
    taskManager.graphNodeStateFor(successor).value should be(GraphNodeState.ORPHAN)
  }

  it should "should resolve an orphan when its predecessor is added" in {
    val predecessor: UnitTask = new ShellCommand("exit", "0") withName "predecessor"
    val successor: UnitTask = new ShellCommand("exit", "0") withName "successor"
    val taskManager: TestTaskManager = getDefaultTaskManager()
    predecessor ==> successor
    taskManager.addTask(successor)
    taskManager.graphNodeStateFor(successor).value should be(GraphNodeState.ORPHAN)
    taskManager.addTask(predecessor)
    taskManager.graphNodeStateFor(predecessor).value should be(GraphNodeState.PREDECESSORS_AND_UNEXPANDED)
    taskManager.stepExecution()
    taskManager.joinOnRunningTasks(1000)
    taskManager.graphNodeStateFor(successor).value should be(GraphNodeState.PREDECESSORS_AND_UNEXPANDED)
    taskManager.graphNodeStateFor(predecessor).value should be(GraphNodeState.RUNNING)
    taskManager.stepExecution()
    taskManager.joinOnRunningTasks(1000)
    taskManager.graphNodeStateFor(successor).value should be(GraphNodeState.RUNNING)
    taskManager.graphNodeStateFor(predecessor).value should be(GraphNodeState.COMPLETED)
    taskManager.stepExecution()
    taskManager.joinOnRunningTasks(1000)
    taskManager.graphNodeStateFor(successor).value should be(GraphNodeState.COMPLETED)
    taskManager.graphNodeStateFor(predecessor).value should be(GraphNodeState.COMPLETED)
  }

  it should "should resolve multiple orphans when their ancestor is added" in {
    val leftA: UnitTask = new ShellCommand("exit", "0") withName "leftA"
    val leftB: UnitTask = new ShellCommand("exit", "0") withName "leftB"
    val rightA: UnitTask = new ShellCommand("exit", "0") withName "leftA"
    val rightB: UnitTask = new ShellCommand("exit", "0") withName "leftB"
    val finalTask: UnitTask = new ShellCommand("exit", "0") withName "finalTask"
    val taskManager: TestTaskManager = getDefaultTaskManager()

    // leftA <- leftB <- finalTask -> rightB -> rightA
    (leftB :: rightB) ==> finalTask
    leftA ==> leftB
    rightA ==> rightB

    // add all but the right A
    taskManager.addTasks(leftA, leftB, rightB, finalTask)

    // run it until we cannot run any more tasks
    taskManager.graphNodeStateFor(rightA) should be ('empty)
    taskManager.graphNodeStateFor(rightB).value should be(GraphNodeState.ORPHAN)
    taskManager.stepExecution()
    taskManager.joinOnRunningTasks(1000)
    taskManager.graphNodeStateFor(leftA).value should be(GraphNodeState.RUNNING)
    taskManager.stepExecution()
    taskManager.joinOnRunningTasks(1000)
    taskManager.graphNodeStateFor(leftA).value should be(GraphNodeState.COMPLETED)
    taskManager.graphNodeStateFor(leftB).value should be(GraphNodeState.RUNNING)
    taskManager.stepExecution()
    taskManager.joinOnRunningTasks(1000)
    taskManager.graphNodeStateFor(leftB).value should be(GraphNodeState.COMPLETED)
    taskManager.graphNodeStateFor(finalTask).value should be(GraphNodeState.PREDECESSORS_AND_UNEXPANDED)
    taskManager.graphNodeStateFor(rightB).value should be(GraphNodeState.ORPHAN)

    // now add right A
    taskManager.addTask(rightA)
    taskManager.stepExecution()
    taskManager.joinOnRunningTasks(1000)
    taskManager.graphNodeStateFor(rightA).value should be(GraphNodeState.RUNNING)
    taskManager.graphNodeStateFor(rightB).value should be(GraphNodeState.PREDECESSORS_AND_UNEXPANDED)
    taskManager.stepExecution()
    taskManager.joinOnRunningTasks(1000)
    taskManager.graphNodeStateFor(rightA).value should be(GraphNodeState.COMPLETED)
    taskManager.graphNodeStateFor(rightB).value should be(GraphNodeState.RUNNING)
    taskManager.stepExecution()
    taskManager.joinOnRunningTasks(1000)
    taskManager.graphNodeStateFor(rightB).value should be(GraphNodeState.COMPLETED)
    taskManager.graphNodeStateFor(finalTask).value should be(GraphNodeState.RUNNING)
    taskManager.stepExecution()
    taskManager.joinOnRunningTasks(1000)
    taskManager.graphNodeStateFor(finalTask).value should be(GraphNodeState.COMPLETED)
  }


  class GetTasksExceptionTask extends Pipeline {
    override def build(): Unit = {
      throw new RuntimeException("GetTasksExceptionTask has excepted")
    }
  }

  it should "mark a task that fails when getTasks is called on it" in {
    val task: Pipeline = new GetTasksExceptionTask()
    val taskManager: TestTaskManager = getDefaultTaskManager()
    taskManager.addTask(task)
    taskManager.stepExecution()
    taskManager.taskStatusFor(task).value should be(TaskStatus.FAILED_GET_TASKS)
  }

  // **************************************************
  // Test dependencies between workflows
  // **************************************************

  class SimplePipeline(name: String) extends Pipeline {
    withName(name)
    val firstTask: UnitTask = new NoOpInJvmTask(taskName=name+"-1")
    val secondTask: UnitTask = new NoOpInJvmTask(taskName=name+"-2")
    root ==> (firstTask :: secondTask) // do this here so we can call getTasks repeatedly
    def build(): Unit = {}
  }

  it should "complete a predecessor pipeline and all ancestors (via getTasks) before the successor pipeline" in {
    val predecessorWorkflow: SimplePipeline = new SimplePipeline("predecessor")
    val successorWorkflow: SimplePipeline = new SimplePipeline("successor")

    predecessorWorkflow.getTasks.size should be(2)
    successorWorkflow.getTasks.size should be(2)

    // no dependencies for either pipeline
    predecessorWorkflow.tasksDependedOn.size should be(0)
    successorWorkflow.tasksDependedOn.size should be(0)

    // predecessor should be added to the tasks for the successor
    predecessorWorkflow ==> successorWorkflow
    successorWorkflow.getTasks.size should be(2)
    predecessorWorkflow.tasksDependedOn.size should be(0)
    successorWorkflow.tasksDependedOn.size should be(1)

    val taskManager: TestTaskManager = getDefaultTaskManager()
    taskManager.addTask(predecessorWorkflow)
    taskManager.addTask(successorWorkflow)

    // Run the scheduler once
    // The predecessor pipeline and its "predecessor" tasks should run, while the successor pipeline should be queued waiting
    // for the predecessor pipeline to finish.
    taskManager.stepExecution()
    taskManager.taskStatusFor(predecessorWorkflow).value should be(TaskStatus.STARTED)
    taskManager.taskStatusFor(predecessorWorkflow.firstTask).value should be(TaskStatus.STARTED)
    taskManager.taskStatusFor(predecessorWorkflow.secondTask).value should be(TaskStatus.STARTED)
    taskManager.graphNodeStateFor(predecessorWorkflow).value should be(GraphNodeState.ONLY_PREDECESSORS)
    taskManager.graphNodeStateFor(predecessorWorkflow.firstTask).value should be(GraphNodeState.RUNNING)
    taskManager.graphNodeStateFor(predecessorWorkflow.secondTask).value should be(GraphNodeState.RUNNING)
    taskManager.taskStatusFor(successorWorkflow.firstTask) should be('empty)
    taskManager.taskStatusFor(successorWorkflow.secondTask) should be('empty)
    taskManager.taskStatusFor(successorWorkflow).value should be(TaskStatus.UNKNOWN)
    taskManager.graphNodeStateFor(successorWorkflow).value should be(GraphNodeState.PREDECESSORS_AND_UNEXPANDED)

    // Run the scheduler again
    // The "predecessor" tasks should be complete, and so the successor pipeline and its "successor" tasks should be running.
    taskManager.joinOnRunningTasks(1000)
    taskManager.stepExecution()
    taskManager.taskStatusFor(predecessorWorkflow).value should be(TaskStatus.SUCCEEDED)
    taskManager.taskStatusFor(predecessorWorkflow.firstTask).value should be(TaskStatus.SUCCEEDED)
    taskManager.taskStatusFor(predecessorWorkflow.secondTask).value should be(TaskStatus.SUCCEEDED)
    taskManager.graphNodeStateFor(predecessorWorkflow).value should be(GraphNodeState.COMPLETED)
    taskManager.graphNodeStateFor(predecessorWorkflow.firstTask).value should be(GraphNodeState.COMPLETED)
    taskManager.graphNodeStateFor(predecessorWorkflow.secondTask).value should be(GraphNodeState.COMPLETED)
    taskManager.taskStatusFor(successorWorkflow).value should be(TaskStatus.STARTED)
    taskManager.taskStatusFor(successorWorkflow.firstTask).value should be(TaskStatus.STARTED)
    taskManager.taskStatusFor(successorWorkflow.secondTask).value should be(TaskStatus.STARTED)
    taskManager.graphNodeStateFor(successorWorkflow).value should be(GraphNodeState.ONLY_PREDECESSORS)
    taskManager.graphNodeStateFor(successorWorkflow.firstTask).value should be(GraphNodeState.RUNNING)
    taskManager.graphNodeStateFor(successorWorkflow.secondTask).value should be(GraphNodeState.RUNNING)

    // Run the scheduler again
    // The successor pipeline and its "successor" tasks should be complete.
    taskManager.joinOnRunningTasks(1000)
    taskManager.stepExecution()
    taskManager.taskStatusFor(successorWorkflow).value should be(TaskStatus.SUCCEEDED)
    taskManager.taskStatusFor(successorWorkflow.firstTask).value should be(TaskStatus.SUCCEEDED)
    taskManager.taskStatusFor(successorWorkflow.secondTask).value should be(TaskStatus.SUCCEEDED)
    taskManager.graphNodeStateFor(successorWorkflow).value should be(GraphNodeState.COMPLETED)
    taskManager.graphNodeStateFor(successorWorkflow.firstTask).value should be(GraphNodeState.COMPLETED)
    taskManager.graphNodeStateFor(successorWorkflow.secondTask).value should be(GraphNodeState.COMPLETED)

    // Run the scheduler a final time
    // There should be no running readyTasks, tasksToSchedule, runningTasks, or completedTasks in this round of scheduling.
    val (readyTasks, tasksToSchedule, runningTasks, completedTasks) = taskManager.stepExecution()
    readyTasks should have size 0
    tasksToSchedule should have size 0
    runningTasks should have size 0
    completedTasks should have size 0

    // Make sure all tasks submitted to the system are completed
    for (taskId <- taskManager.taskIds()) {
      taskManager.taskStatusFor(taskId).value should be (TaskStatus.SUCCEEDED)
      taskManager.graphNodeStateFor(taskId).value should be(GraphNodeState.COMPLETED)
    }
    taskManager.taskIds() should have size 6
  }

  // **************************************************
  // Test cycles in the dependency graph
  // **************************************************

  {
    it should "fail to add a task that has a cyclical dependency" in {
      val predecessor: Task = new NoOpTask withName "predecessor"
      val successor: Task = new NoOpTask withName "successor"

      // make a simple cycle
      predecessor ==> successor ==> predecessor

      val taskManager: TestTaskManager = getDefaultTaskManager()
      an[IllegalArgumentException] should be thrownBy taskManager.addTask(predecessor)
    }

    // TODO: This test should fail due the introduction of a cycle during the pipeline.build(), but doesn't
//    it should "fail to run a task if it is found to have a cyclical dependency after getTasks is called" in {
//
//      // Set things up so that task ==> pipeline, and when pipeline.build() is called we also introduce
//      // a dependency from a pipeline_task ==> task
//      val task: Task = new NoOpTask withName "StandaloneUnitTask"
//      val naughtyPipeline = new Pipeline {
//        override def build(): Unit = {
//          val innerTask = new NoOpTask().withName("UnitTaskInPipeline")
//          root ==> innerTask
//          task ==> innerTask
//        }
//      }
//
//      naughtyPipeline ==> task
//
//      naughtyPipeline.getTasksDependedOn should have size 0
//      naughtyPipeline.getTasksDependingOnThisTask should have size 1
//      task.getTasksDependedOn should have size 1
//      task.getTasksDependingOnThisTask should have size 0
//
//      val taskManager: TestTaskManager = getDefaultTaskManager()
//      taskManager.addTask(naughtyPipeline) should be(0)
//      taskManager.addTask(task) should be(1)
//
//      // getTasks should be called on predecessor, which now creates a cycle!
//      taskManager.runSchedulerOnce()
//      taskManager.getTaskStatus(naughtyPipeline).value should be(TaskStatus.FAILED_GET_TASKS)
//      taskManager.getTaskStatus(task).value should be(TaskStatus.UNKNOWN)
//      taskManager.getGraphNodeState(naughtyPipeline).value should be(GraphNodeState.COMPLETED)
//      taskManager.getGraphNodeState(task).value should be(GraphNodeState.PREDECESSORS_AND_UNEXPANDED)
//    }
  }

  // Test that scheduler limits are observed!
  {
    it should "not run tasks concurrently with more Cores than are defined in the system." in {
      val systemCores       = 4
      var allocatedCores    = 0
      var maxAllocatedCores = 0

      // A task that would like 1-8 cores each
      class HungryTask extends ProcessTask {
        var coresGiven = 0
        override def args = "exit" :: "0" :: Nil

        override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
          (8 to 1 by -1).map{ c => ResourceSet(Cores(c), Memory("1g")) }.find(rs => availableResources.subset(rs).isDefined)
        }

        override def applyResources(resources: ResourceSet): Unit = {
          coresGiven = resources.cores.toInt
          allocatedCores += coresGiven
          maxAllocatedCores = Math.max(maxAllocatedCores, allocatedCores)
        }

        override def onComplete(exitCode: Int): Boolean = {
          allocatedCores -= coresGiven
          super.onComplete(exitCode)
        }
      }

      // A pipeline with several hungry tasks that can be run in parallel
      class HungryPipeline extends Pipeline {
        override def build(): Unit = root ==> (new HungryTask :: new HungryTask :: new HungryTask)
      }

      TaskManager.run(
        new HungryPipeline,
        sleepMilliseconds = 1,
        taskManagerResources = Some(SystemResources(systemCores, Resource.parseSizeToBytes("8g").toLong, 0.toLong)),
        failFast=true
      )
      maxAllocatedCores should be <= systemCores
    }
  }

  it should "handle a few thousand tasks" taggedAs TestTags.LongRunningTest in {
    val numTasks = 10000
    val dependencyProbability = 0.1

    class ATask extends ProcessTask {
      override def args = "exit" :: "0" :: Nil

      override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
        val mem = Memory("1g")
        (8 to 1 by -1).map(c => ResourceSet(Cores(c), mem)).find(rs => availableResources.subset(rs).isDefined)
      }
    }

    // create the tasks
    val tasks = for (i <- 1 to numTasks) yield new ATask

    // make them depend on previous tasks
    val randomNumberGenerator = scala.util.Random
    for (i <- 1 until numTasks) {
      for (j <- 1 until i) {
        if (randomNumberGenerator.nextFloat < dependencyProbability) tasks(j) ==> tasks(i)
      }
    }

    // add the tasks to the task manager
    val taskManager: TestTaskManager = getDefaultTaskManager(sleepMilliseconds = 1)
    taskManager.addTasks(tasks)

    // run the tasks
    taskManager.runToCompletion(failFast=true)

    // make sure all tasks have been completed
    tasks.foreach { task =>
      taskManager.taskStatusFor(task).value should be(TaskStatus.SUCCEEDED)
      taskManager.graphNodeStateFor(task).value should be(GraphNodeState.COMPLETED)
    }
  }

  private def getAndTestTaskExecutionInfo(taskManager: TestTaskManager, task: Task): TaskExecutionInfo = {
    // make sure the execution info and timestamps are set for this taske
    val info = taskManager.taskExecutionInfoFor(task)
    info shouldBe 'defined
    info.get.submissionDate shouldBe 'defined
    info.get.startDate shouldBe 'defined
    info.get.endDate shouldBe 'defined
    info.get
  }

  it should "set the submission, start, and end dates correctly for Pipelines" in {
    val taskOne = new ShellCommand("sleep", "1")
    val taskTwo = new ShellCommand("sleep", "1")
    val pipeline = new Pipeline {
      override def build(): Unit = {
        root ==> taskOne ==> taskTwo
      }
    }

    // add the tasks to the task manager
    val taskManager: TestTaskManager = getDefaultTaskManager(sleepMilliseconds = 1)
    taskManager.addTasks(pipeline)

    // run the tasks
    taskManager.runToCompletion(failFast=true)

    // get and check the info
    val pipelineInfo: TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, pipeline)
    val taskOneInfo : TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, taskOne)
    val taskTwoInfo : TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, taskTwo)

    // the submission and start date should match the first task, but the end date should be after since the second task
    // ends after the first
    pipelineInfo.submissionDate.get.compareTo(taskOneInfo.submissionDate.get) should be <= 0
    pipelineInfo.startDate.get.equals(taskOneInfo.startDate.get) shouldBe true
    pipelineInfo.endDate.get.isAfter(taskOneInfo.endDate.get) shouldBe true

    // the end date should match the second task, but the second task is started after the first
    pipelineInfo.submissionDate.get.compareTo(taskTwoInfo.submissionDate.get) should be <= 0
    pipelineInfo.startDate.get.isBefore(taskTwoInfo.startDate.get) shouldBe true
    pipelineInfo.endDate.get.equals(taskTwoInfo.endDate.get) shouldBe true
  }

  it should "set the submission, start, and end dates correctly for a Pipeline within a Pipeline" in {
    val firstTask = new ShellCommand("sleep", "1") // need to wait to make sure timestamps are updated
    val secondTask = new ShellCommand("sleep", "1") // need to wait to make sure timestamps are updated
    val innerPipeline = new Pipeline {
      override def build(): Unit = {
        root ==> secondTask
      }
    }
    val outerPipeline = new Pipeline {
      override def build(): Unit = {
        root ==> firstTask ==> innerPipeline
      }
    }
    // NB: the execution is really: root ==> firstTask ==> secondTask

    // add the tasks to the task manager
    val taskManager: TestTaskManager = getDefaultTaskManager(sleepMilliseconds = 1)
    taskManager.addTasks(outerPipeline)

    // run the tasks
    taskManager.runToCompletion(failFast=true)

    // get and check the info
    val firstTaskInfo : TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, firstTask)
    val secondTaskInfo : TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, secondTask)
    val innerPipelineInfo: TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, innerPipeline)
    val outerPipelineInfo: TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, outerPipeline)

    // submission dates
    // inner pipeline is submitted just before second task, while outer pipeline is submitted just before first task
    innerPipelineInfo.submissionDate.get.compareTo(secondTaskInfo.submissionDate.get) should be <= 0
    outerPipelineInfo.submissionDate.get.compareTo(firstTaskInfo.submissionDate.get) should be <= 0

    // start dates
    // - inner pipeline should have a start date of task two, while outer pipeline should have a start date of task one
    innerPipelineInfo.startDate.get.compareTo(secondTaskInfo.startDate.get) should be <= 0
    outerPipelineInfo.startDate.get.compareTo(firstTaskInfo.startDate.get) should be <= 0

    // end dates
    // - both pipelines should have end dates of task two
    innerPipelineInfo.endDate.get.compareTo(secondTaskInfo.endDate.get) should be <= 0
    outerPipelineInfo.endDate.get.compareTo(secondTaskInfo.endDate.get) should be <= 0
  }

  private class ParentFailTask extends Task {
    val child: Task = new ShellCommand("exit", "1")
    override def getTasks: Traversable[_ <: Task] = Seq(child)
  }

  it should "mark a task as failed when one of its children fails" in {
    val parent = new ParentFailTask()
    val taskManager: TestTaskManager = getDefaultTaskManager(sleepMilliseconds = 1)
    taskManager.addTask(parent)
    taskManager.runToCompletion(failFast=true)
    Seq(parent.child, parent).foreach { task =>
      val info : TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, task)
      info.status shouldBe TaskStatus.FAILED_COMMAND
    }
  }

  private class FailPipeline(val child: Task = new ShellCommand("exit", "1")) extends Pipeline {
    def build(): Unit = root ==> child
  }

  it should "mark a pipeline as failed when one of its children fails" in {
    val pipeline = new FailPipeline()
    val taskManager: TestTaskManager = getDefaultTaskManager(sleepMilliseconds = 1)
    taskManager.addTask(pipeline)
    taskManager.runToCompletion(failFast=true)
    Seq(pipeline.child, pipeline).foreach { task =>
      val info : TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, task)
      info.status shouldBe TaskStatus.FAILED_COMMAND
    }
  }

  it should "propagate failures back to parent tasks" in {
    val leaf: Task = new ShellCommand("exit", "1") withName "Leaf"
    val tasks = ListBuffer[Task]()
    tasks += leaf
    val root: Task = Range.inclusive(1, 10).foldLeft(leaf) { case (child, index) =>
      val pipeline = new FailPipeline(child=child) withName s"Fail.$index"
      tasks += pipeline
      pipeline
    }
    val taskManager: TestTaskManager = getDefaultTaskManager(sleepMilliseconds = 1)
    taskManager.addTask(root)
    taskManager.runToCompletion(failFast=true)
    tasks.foreach { task =>
      val info : TaskExecutionInfo = getAndTestTaskExecutionInfo(taskManager, task)
      info.status shouldBe TaskStatus.FAILED_COMMAND
    }
  }
}
