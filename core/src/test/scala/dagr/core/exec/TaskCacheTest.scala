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

import dagr.core.UnitSpec
import dagr.core.tasksystem.{NoOpTask, Task}

import scala.collection.mutable.ListBuffer

class TaskCacheTest extends UnitSpec {

  /** A simple task cache to test the [[TaskCache]] interface. */
  class TestTaskCache extends TaskCache {
    private val _roots = ListBuffer[Task]()
    private val _children = ListBuffer[Task]()
    private val _toExecute = ListBuffer[Task]()
    def roots: Seq[Task] = this._roots
    def children: Seq[Task] = this._children
    def taskSet: Set[Task] = (this.roots ++ this.children).toSet
    // NB: will only return true if a root task with no children was registered
    def execute(task: Task): Boolean = this._toExecute.contains(task)
    protected def registered(task: Task): Boolean = tasks.contains(task)
    protected def registerRoot(root: Task): Unit = if (!this._roots.contains(root)) this._roots += root
    protected def registerChildren(parent: Task, children: Seq[Task]): Unit = children.foreach { child =>
      if (!this._children.contains(child)) this._children += child
    }
    override protected def checkStatusAndExecute(task: Task): Unit = if (!this._toExecute.contains(task)) this._toExecute += task
  }

  "TaskCache" should "register a root task" in {
    val cache = new TestTaskCache()
    val task  = new NoOpTask
    cache.register(task)
    cache.register(task, task)
    cache.roots should contain theSameElementsInOrderAs Seq(task)
    cache.children shouldBe 'empty
    cache.tasks should contain theSameElementsInOrderAs Seq(task)
    cache.execute(task) shouldBe true // since it was a root task with no children
  }

  it should "register a unit task" in {
    val cache = new TestTaskCache()
    val root  = new NoOpTask
    val task  = new NoOpTask
    cache.register(root) // register the root
    cache.register(root, task) // register the task relative to the root
    cache.register(task, task) // register the task
    cache.roots should contain theSameElementsInOrderAs Seq(root)
    cache.children should contain theSameElementsInOrderAs Seq(task)
    cache.tasks should contain theSameElementsInOrderAs Seq(root, task)
  }

  it should "register a task that builds other tasks" in {
    val cache = new TestTaskCache()
    val root  = new NoOpTask
    val children = Seq(new NoOpTask, new NoOpTask)
    cache.register(root)
    cache.register(root, children:_*)
    cache.roots shouldBe Seq(root)
    cache.children should contain theSameElementsInOrderAs children
    cache.tasks should contain theSameElementsInOrderAs root +: children
  }

  it should "register various tasks" in {
    val cache = new TestTaskCache()
    val root  = new NoOpTask
    val children = Seq(new NoOpTask, new NoOpTask)

    // root
    cache.register(root)
    // children
    cache.register(root, children:_*)
    // children themselves
    children.foreach { child => cache.register(child, child) }

    // check it
    cache.roots should contain theSameElementsInOrderAs Seq(root)
    cache.children shouldBe children
    cache.tasks should contain theSameElementsInOrderAs Seq(root) ++ children
  }
}
