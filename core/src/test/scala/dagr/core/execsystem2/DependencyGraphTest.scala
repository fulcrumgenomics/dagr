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

package dagr.core.execsystem2

import dagr.core.UnitSpec
import dagr.core.tasksystem.{NoOpInJvmTask, Task}
import org.scalatest.OptionValues

class DependencyGraphTest extends UnitSpec with OptionValues {

  def dependencyGraph: DependencyGraph = DependencyGraph()
  def task: Task = new NoOpInJvmTask("noop")

  "DependencyGraph.add" should "add a task to the graph that has no dependencies" in {
    val graph = this.dependencyGraph
    val task = this.task
     graph.add(task) shouldBe true
  }

  it should "not throw an exception when there are cycles in the graph" in {
    val graph = this.dependencyGraph
    val root = this.task withName "root"
    val child = this.task withName "child"
    root ==> child ==> root
    graph.add(root) shouldBe false
    graph.add(child) shouldBe false
  }

  it should "add a task that has dependencies" in {
    val graph = this.dependencyGraph
    val root = this.task withName "root"
    val child = this.task withName "child"
    root ==> child
    graph.add(root) shouldBe true
    graph.add(child) shouldBe false
  }

  "DependencyGraph.remove" should "throw an exception if the task to be removed still has dependencies" in {
    val graph = this.dependencyGraph
    val root = this.task withName "root"
    val child = this.task withName "child"
    root ==> child
    graph.add(root)
    graph.add(child)
    an[IllegalArgumentException] should be thrownBy graph.remove(child)
  }

  it should "remove all dependencies" in {
    val graph = this.dependencyGraph
    val root = this.task withName "root"
    val child = this.task withName "child"
    root ==> child
    graph.add(root) shouldBe true
    graph.add(child) shouldBe false

    graph.remove(root) should contain theSameElementsInOrderAs Seq(child)
    graph.hasDependencies(child).value shouldBe false
    graph.remove(child) shouldBe 'empty
  }

  it should "throw an exception if a dependent is not found" in {
    val graph = this.dependencyGraph
    val root = this.task withName "root"
    val child = this.task withName "child"
    root ==> child
    graph.add(root) shouldBe true

    an[IllegalArgumentException] should be thrownBy graph.remove(root)
  }

  "DependencyGraph.hasDependencies" should "should return None when a task is not part of the graph" in {
    val graph = this.dependencyGraph
    val task = this.task
    graph.hasDependencies(task) shouldBe 'empty
  }

  it should "return Some(false) when a task has no dependencies" in {
    val graph = this.dependencyGraph
    val task = this.task
    graph.add(task) shouldBe true
    graph.hasDependencies(task).value shouldBe false
  }

  it should "return Some(true) when a task has some dependencies" in {
    val graph = this.dependencyGraph
    val root = this.task withName "root"
    val child = this.task withName "child"
    root ==> child
    graph.add(root) shouldBe true
    graph.add(child) shouldBe false
    graph.hasDependencies(root).value shouldBe false
    graph.hasDependencies(child).value shouldBe true
  }

  "DependencyGraph.contains" should "should return false when a task is not part of the graph" in {
    val graph = this.dependencyGraph
    val task = this.task
    graph.contains(task) shouldBe false
  }

  it should "should return true when a task is part of the graph" in {
    val graph = this.dependencyGraph
    val task = this.task
    graph.add(task) shouldBe true
    graph.contains(task) shouldBe true
  }

  "DependencyGraph.size" should "should return the number of tasks in the graph" in {
    val graph = this.dependencyGraph
    val root = this.task withName "root"
    val child = this.task withName "child"
    root ==> child
    graph.add(root) shouldBe true
    graph.size shouldBe 1
    graph.add(child) shouldBe false
    graph.size shouldBe 2
  }

  "DependencyGraph.exceptIfCyclicalDependency" should "throw an exception when there are cycles in the graph" in {
    val graph = this.dependencyGraph
    val root = this.task withName "root"
    val child = this.task withName "child"
    root ==> child ==> root
    graph.add(root) shouldBe false
    graph.add(child) shouldBe false
    an[IllegalArgumentException] should be thrownBy graph.exceptIfCyclicalDependency(root)
    an[IllegalArgumentException] should be thrownBy graph.exceptIfCyclicalDependency(child)

  }

  "DependencyGraph" should "do some operations" in {
    val graph = this.dependencyGraph
    val root = this.task withName "root"
    val left = this.task withName "left"
    val right = this.task withName "right"
    val leaf = this.task withName "leaf"
    root ==> (left :: right) ==> leaf
    val tasks = Seq(root, left, right, leaf)

    // add to the graph all but the leaf
    Seq(root, left, right).foreach { task =>
      graph.add(task) shouldBe (task == root)
    }
    graph.size shouldBe 3
    Seq(root, left, right).foreach { task =>
      graph.hasDependencies(task).value shouldBe (task != root)
    }
    graph.hasDependencies(leaf) shouldBe None

    // remove root
    graph.remove(root) should contain theSameElementsInOrderAs Seq(left, right)
    // add the leaf
    graph.add(leaf) shouldBe false
    graph.size shouldBe 4
    Seq(left, right, leaf).foreach { task =>
      graph.hasDependencies(task).value shouldBe (task == leaf)
    }

    // remove left
    graph.remove(left) shouldBe 'empty
    graph.size shouldBe 4
    Seq(right, leaf).foreach { task =>
      graph.hasDependencies(task).value shouldBe (task == leaf)
    }

    // remove right
    graph.remove(right) should contain theSameElementsInOrderAs Seq(leaf)
    graph.size shouldBe 4
    graph.hasDependencies(leaf).value shouldBe false

    // remove leaf
    graph.remove(leaf) shouldBe 'empty
    graph.size shouldBe 4

    tasks.foreach { task =>
      graph.hasDependencies(task).value shouldBe false
    }
  }

}
