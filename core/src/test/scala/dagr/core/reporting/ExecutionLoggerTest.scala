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

package dagr.core.reporting

import java.nio.file.Files

import com.fulcrumgenomics.commons.io.Io
import dagr.core.FutureUnitSpec
import dagr.core.exec.{Executor, SystemResources}
import dagr.core.execsystem.{TaskStatus => ExecTaskStatus}
import dagr.core.execsystem2.{TaskStatus => Exec2TaskStatus}
import dagr.core.reporting.ExecutionLogger.{Definition, Relationship, Status}
import dagr.core.tasksystem.{NoOpInJvmTask, Pipeline, SimpleInJvmTask, Task}

// Defining this here because it cannot be an inner class: https://bugs.openjdk.java.net/browse/JDK-8057919
private object TestTrait {
  case object ObjectTask extends SimpleInJvmTask {
    override def run(): Unit = Unit
  }
}

class ExecutionLoggerTest extends FutureUnitSpec {

  case object ObjectTask extends SimpleInJvmTask {
    override def run(): Unit = Unit
  }

  case object AnotherObjectTask extends SimpleInJvmTask {
    override def run(): Unit = Unit
  }

  "ExecutionLogger.Definition.getSimpleName" should "return the simple class name without any $ characters" in {
    Definition.getSimpleName(ObjectTask) shouldBe "ObjectTask"
    Definition.getSimpleName(new NoOpInJvmTask("SomeName")) shouldBe "NoOpInJvmTask"
  }

  "ExecutionLogger.Definition.buildRootDefinition" should "return a definition with parent code -1 and child code 0" in {
    Definition.buildRootDefinition(ObjectTask) shouldBe Definition(ObjectTask, -1, 0)
  }

  "ExecutionLogger.Definition.toString" should "should return a comma-separated list of the values" in {
    Definition(ObjectTask, 10, 11).toString shouldBe s"DEFINITION,ObjectTask,ObjectTask$$,${ObjectTask.hashCode()},10,11"
  }

  "ExecutionLogger.Definition.apply" should "should parse from a string" in {
    Definition(s"DEFINITION,ObjectTask,ObjectTask$$,${ObjectTask.hashCode()},10,11") shouldBe Definition(ObjectTask, 10, 11)
  }

  it should "throw an exception on a garbage string" in {
    an[Exception] should be thrownBy Definition("Garbage string")
  }

  "ExecutionLogger.Definition.equivalent" should "should return true if two definitions the same task, false otherwise" in {
    val definitionA = Definition(task=ObjectTask,           parentCode=this.hashCode(),              childNumber=42)
    val definitionB = Definition(task=TestTrait.ObjectTask, parentCode=this.hashCode(),              childNumber=42)
    val definitionC = Definition(task=AnotherObjectTask,    parentCode=this.hashCode(),              childNumber=42)
    val definitionD = Definition(task=AnotherObjectTask,    parentCode=this.hashCode(),              childNumber=41)
    val definitionE = Definition(task=AnotherObjectTask,    parentCode=AnotherObjectTask.hashCode(), childNumber=41)

    val allDefinitions = Seq(definitionA, definitionB, definitionC, definitionD, definitionE)

    allDefinitions.foreach { definition =>
      definition.equivalent(definition)
    }

    definitionA.equivalent(definitionB) shouldBe true
    definitionA.equivalent(definitionC) shouldBe false
    definitionA.equivalent(definitionD) shouldBe false
    definitionA.equivalent(definitionE) shouldBe false

    definitionB.equivalent(definitionA) shouldBe true
    definitionB.equivalent(definitionC) shouldBe false
    definitionB.equivalent(definitionD) shouldBe false
    definitionB.equivalent(definitionE) shouldBe false

    Seq(definitionC, definitionD, definitionE).foreach { leftDefinition =>
      allDefinitions.foreach { rightDefinition =>
        if (leftDefinition != rightDefinition) {
          leftDefinition.equivalent(rightDefinition) shouldBe false
        }
      }
    }
  }

  "ExecutionLogger.Relationship.toString" should "should return a comma-separated list of the values" in {
    val definitionParent = Definition.buildRootDefinition(ObjectTask)
    val definitionChild  = Definition(task=AnotherObjectTask,    parentCode=definitionParent.code, childNumber=0)
    val relationship     = Relationship(parent=definitionParent, child=definitionChild)
    relationship.toString shouldBe s"RELATIONSHIP,${ObjectTask.hashCode},${AnotherObjectTask.hashCode}"
  }

  "ExecutionLogger.Relationship.apply" should "should parse from a string" in {
    Relationship("RELATIONSHIP,123,456") shouldBe Relationship(123, 456)

  }

  it should "throw an exception on a garbage string" in {
    an[Exception] should be thrownBy Relationship("Garbage string")
  }

  it should "throw an exception if the parent code of the child was not equal to the given parent code" in {
    val definitionParent = Definition.buildRootDefinition(ObjectTask)
    val definitionChild  = definitionParent.copy(parentCode=definitionParent.code+1)
    an[Exception] should be thrownBy Relationship(parent=definitionParent, child=definitionChild)
  }

  "ExecutionLogger.Status.toString" should "should return a comma-separated list of the values" in {
    val definition = Definition.buildRootDefinition(ObjectTask)
    val status     = Status(ExecTaskStatus.SucceededExecution, definition)
    val succeeded  = ExecTaskStatus.SucceededExecution
    status.toString shouldBe s"STATUS,${definition.code},${succeeded.name},${succeeded.ordinal}"
  }

  "ExecutionLogger.Status.apply" should "should parse from a string" in {
    val failedGetTasks = ExecTaskStatus.FailedGetTasks
    Status(s"STATUS,123,FailedGetTasks,${failedGetTasks.ordinal}") shouldBe Status(123, failedGetTasks.name, failedGetTasks.ordinal)
  }

  it should "throw an exception on a garbage string" in {
    an[Exception] should be thrownBy Status("Garbage string")
  }

  // TODO: test all executors!


  "ExecutionLogger" should "write an execution log when no tasks are logged" in {
    Seq(true, false).foreach { experimentalExecution =>
      val log = {
        val path = Files.createTempFile("execution.", ".log")
        path.toFile.deleteOnExit()
        path
      }

      val logger = new ExecutionLogger(log)
      logger.close()

      val lines = Io.readLines(log).toSeq

      lines.length shouldBe 3
      lines.forall(_.startsWith("#")) shouldBe true
    }
  }

  it should "write an execution log when many tasks are executed" in {
    Seq(true, false).foreach { experimentalExecution =>
      val log = {
        val path = Files.createTempFile("execution.", ".log")
        path.toFile.deleteOnExit()
        path
      }

      def task(_name: String): Task = new NoOpInJvmTask(_name)
      def fail(_name: String): Task = new SimpleInJvmTask {
        this.name = _name
        override def run(): Unit = throw new IllegalArgumentException("purposely failing")
      }

      // Make up some tasks
      // - p1.c1 fails since it cannot be scheduled
      // - p2.c4 fails since it throws an exception
      val pipeline1 = new Pipeline(prefix=Some("p1.")) {
        name = "p1"
        override def build(): Unit = {
          root ==> task("c1") ==> fail("c2") ==> task("c3")
        }
      }
      val pipeline = new Pipeline() {
        this.name = "p"
        override def build(): Unit = {
          val first = task("first")
          val leaf  = task("leaf")
          this.root ==> first ==> pipeline1 ==> leaf
        }
      }

      // Run it
      val logger = new ExecutionLogger(log)
      val executor = Executor(experimentalExecution=experimentalExecution, resources=SystemResources(1, Long.MaxValue, Long.MaxValue))
      executor.withReporter(logger)
      executor.execute(pipeline) shouldBe 5 // p, p1, p1.c2, p1.c3, leaf
      logger.close()

      // Header
      val lines = Io.readLines(log).toSeq
      lines.count(_.startsWith("#")) shouldBe 3

      // Definitions
      val definitions = lines.filter(_.startsWith("DEFINITION")).map(Definition(_))
      definitions.length shouldBe 7

      // Relationships
      val relationships = lines.filter(_.startsWith("RELATIONSHIP")).map(Relationship(_))
      relationships.length shouldBe 6

      // Statuses
      val statuses = lines.filter(_.startsWith("STATUS")).map(Status(_))

      val counts = if (experimentalExecution){
        Map(
          Exec2TaskStatus.Pending            -> 7,
          Exec2TaskStatus.Queued             -> 5,
          Exec2TaskStatus.Running            -> 5,
          Exec2TaskStatus.Submitted          -> 3,
          Exec2TaskStatus.FailedExecution    -> 3,
          Exec2TaskStatus.SucceededExecution -> 2
        )
      }
      else {
        Map(
          ExecTaskStatus.Unknown            -> 7,
          ExecTaskStatus.Started            -> 5,
          ExecTaskStatus.FailedExecution    -> 1,
          ExecTaskStatus.SucceededExecution -> 2
        )
      }

      counts.foreach { case (status, count) =>
        statuses.count(_.statusOrdinal == status.ordinal) shouldBe count
        statuses.count(_.statusName == status.name) shouldBe count
      }

      statuses.length shouldBe counts.values.sum
    }
  }
}
