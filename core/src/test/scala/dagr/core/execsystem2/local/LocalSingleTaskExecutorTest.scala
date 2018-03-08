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

package dagr.core.execsystem2.local

import java.nio.file.{Files, Path}

import com.fulcrumgenomics.commons.CommonsDef.FilePath
import dagr.api.models.util.ResourceSet
import dagr.core.FutureUnitSpec
import dagr.core.execsystem2.TaskInfo
import dagr.core.execsystem2.TaskStatus.Running
import dagr.core.tasksystem._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, CancellationException, TimeoutException}


class LocalSingleTaskExecutorTest extends FutureUnitSpec {

  /** A task that exits zero and that's it. */
  private class TrivialInJvmTask(exitCode: Int, sleep: Int = 0) extends InJvmTask with FixedResources {
    override def inJvmMethod(): Int = {
      Thread.sleep(sleep)
      exitCode
    }
  }

  /** A [[dagr.core.tasksystem.ProcessTask]] that throws an exception when trying to build a process builder. */
  private class ProcessBuilderExceptionTask extends ProcessTask with FixedResources {
    override def processBuilder(script: Path, logFile: Path, setPipefail: Boolean = true): scala.sys.process.ProcessBuilder = {
      throw new IllegalStateException("I failed creating my process builder")
    }
    override def onComplete(exitCode: Int): Boolean = false
    override def args: Seq[Any] = List.empty
  }

  private class ProperShellCommand(commands: String*) extends ShellCommand(commands:_*) {
    this.quoteIfNecessary = false
  }

  private trait FailOnCompleteTask extends UnitTask {
    override def onComplete(exitCode: Int): Boolean = false
  }

  private trait OnCompleteIsOppositeExitCodeTask extends UnitTask {
    override def onComplete(exitCode: Int): Boolean = 0 != exitCode
  }

  private class InJvmExceptionTask extends SimpleInJvmTask {
    override def run() = throw new IllegalStateException("I throw exceptions")
  }

  /** A path to a script file. */
  private def script: FilePath = Files.createTempFile("TaskRunnerTest", ".sh")

  /** A path to a log file. */
  private def log: FilePath    = Files.createTempFile("TaskRunnerTest", ".log")

  /** Creates a task runner. */
  private def taskRunner(task: UnitTask): LocalSingleTaskExecutor = {
    new TaskInfo(task=task, initStatus=Running)
    task.taskInfo.script = Some(script)
    task.taskInfo.log    = Some(log)
    LocalSingleTaskExecutor(task)
  }

  /** Create a trivial in JVM task and build it. */
  private def trivialInJvmTask(exitCode: Int): InJvmTask = new TrivialInJvmTask(exitCode = exitCode).getTasks.head

  /** Create a trivial in process task and build it. */
  private def proccessTask(argv: List[String]): ProcessTask =  new ShellCommand(argv:_*).withName(argv.mkString(" ")).getTasks.head

  /** Create a in JVM or process task that will exit with zero or one, and then build it. */
  private def testTask(doInJvmTask: Boolean=true, exitOk: Boolean=true, onCompleteSuccessful: Boolean=true, sleep: Int=0): UnitTask = {
    val exitCode = if (exitOk) 0 else 1
    val task = {
      if (doInJvmTask) {
        if (onCompleteSuccessful) new TrivialInJvmTask(exitCode=exitCode, sleep=sleep)
        else new TrivialInJvmTask(exitCode=exitCode, sleep=sleep) with FailOnCompleteTask
      }
      else {
        val argv = Seq("sleep", sleep, ";", "exit", exitCode).map(_.toString)
        if (onCompleteSuccessful) new ProperShellCommand(argv:_*)
        else new ProperShellCommand(argv:_*) with FailOnCompleteTask
      }
    }
    task.withName(s"Exit $exitCode").getTasks.head
  }

  /** Build a task and task runner. */
  private def taskRunner(doInJvmTask: Boolean, exitOk: Boolean=true, onCompleteSuccessful: Boolean=true, sleep: Int=0): LocalSingleTaskExecutor = {
    val task = testTask(doInJvmTask=doInJvmTask, exitOk=exitOk, onCompleteSuccessful=onCompleteSuccessful, sleep=sleep)
    taskRunner(task=task)
  }

  /** Call the execute() method for at most the duration, and return a future. */
  private def executeRunner(runner: LocalSingleTaskExecutor, duration: Duration = Duration("60s")): UnitTask = {
    Await.result(runner.execute(), duration)
  }

  private def requireRunner(runner: LocalSingleTaskExecutor,
                            exitCode: Int= 0,
                            onCompleteSuccessful: Option[Boolean] = Some(true),
                            thrown: Boolean = false): Unit = {
    runner.exitCode.value shouldBe exitCode
    runner.throwable.nonEmpty shouldBe thrown
  }

  Seq(true, false).foreach { doInJvmTask =>
    val doInJvmTaskMsg = if (doInJvmTask) "in the JVM" else "in a Process"

    s"LocalSingleTaskExecutor ($doInJvmTaskMsg)" should "run with exit 0 and succeed" in {
      val runner = taskRunner(doInJvmTask=doInJvmTask)
      executeRunner(runner) shouldBe runner.task
      requireRunner(runner=runner)
    }

    it should "run with exit 1 and fail" in {
      val runner = taskRunner(doInJvmTask=doInJvmTask, exitOk=false)
      executeRunner(runner) shouldBe runner.task
      requireRunner(runner=runner, exitCode=1)
    }

    it should "have a task fail its onComplete method" in {
      val runner = taskRunner(doInJvmTask=doInJvmTask, onCompleteSuccessful=false)
      executeRunner(runner) shouldBe runner.task
      requireRunner(runner=runner, onCompleteSuccessful=Some(false))
    }

    it should "have a task that fail on its onComplete method only if the exit code is zero" in {
      {
        val runner = taskRunner(doInJvmTask=doInJvmTask, onCompleteSuccessful=false)
        executeRunner(runner) shouldBe runner.task
        requireRunner(runner=runner, onCompleteSuccessful=Some(false))
      }
      {
        val runner = taskRunner(doInJvmTask=doInJvmTask, exitOk=false)
        executeRunner(runner) shouldBe runner.task
        requireRunner(runner=runner, exitCode=1)
      }
    }

    it should "have a special exit code when a task gets interrupted" in {
      val runner = taskRunner(doInJvmTask=doInJvmTask, sleep=100000) // sleep forever
      val future = runner.execute()
      future.isCompleted shouldBe false
      runner.interrupt() shouldBe None
      future.isCompleted shouldBe true
      runner.join(Duration("1s")) shouldBe None // interrupted!
      runner.interrupted() shouldBe true
      requireRunner(runner=runner, exitCode=LocalSingleTaskExecutor.InterruptedExitCode, thrown=true)
      runner.throwable.value shouldBe a[CancellationException]
      whenReady(future.failed) { thr =>
        thr shouldBe a[CancellationException]
      }
    }

    it should "have a special exit code when a task does not join in the specified time" in {
      val runner = taskRunner(doInJvmTask=doInJvmTask, sleep=100000) // sleep forever
      val future = runner.execute()
      future.isCompleted shouldBe false
      runner.join(Duration("1s")) shouldBe None // timeout!
      requireRunner(runner=runner, exitCode=LocalSingleTaskExecutor.InterruptedExitCode, thrown=true)
      runner.throwable.value shouldBe a[TimeoutException]
    }
  }

  "LocalTaskRunner" should "have an non-zero exit code when an InJvmTask that throws an Exception" in {
    val task = new InJvmExceptionTask
    val runner = taskRunner(task)
    executeRunner(runner) shouldBe runner.task
    requireRunner(runner=runner, exitCode=1)
  }

  it should "get a non-zero exit code for a task that fails during getProcessBuilder by throwing an exception" in {
    val task   = new ProcessBuilderExceptionTask
    val runner = taskRunner(task)
    executeRunner(runner) shouldBe runner.task
    requireRunner(runner=runner, exitCode=1, onCompleteSuccessful=Some(false), thrown=true)
  }

  it should "fail to run a UnitTask that is not either an InJvmTask or a ProcessTask task" in  {
    val task = new UnitTask {
      override def pickResources(availableResources: ResourceSet) = None
    }
    an[RuntimeException] should be thrownBy taskRunner(task)
  }
}
