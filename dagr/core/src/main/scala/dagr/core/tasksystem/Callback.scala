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

/**
  * A simple callback trait, instances of which can be registered with Tasks. Registered callbacks
  * will be invoked by the server immediately before invoking getTasks.
  */
trait Callback {
  def invoke() : Unit
}

/** Provides facilities for adding common callbacks to Tasks. */
object Callbacks {
  /** Connects two tasks with the provided function. */
  def connect[ChildType <: Task, ParentType <: Task](child: ChildType, parent: ParentType)(f: (ChildType, ParentType) => Unit): Unit = {
    val conn = new Connector(child, parent)(f)
    child.addCallback(conn)
  }

  /** Connects a child task with any number of parent or upstream tasks. */
  def connect[ChildType <: Task, ParentType <: Task](child: ChildType, parents: TraversableOnce[ParentType])(f: (ChildType, List[ParentType]) => Unit): Unit = {
    val conn = new ManyToOneConnector(child, parents.toList)(f)
    child.addCallback(conn)
  }
}

/**
  * A type of Callback that is designed to "connect" a pair of tasks with a function that is given
  * access to both the child task and the parent task.
  */
protected class Connector[ChildType <: Task, ParentType <: Task](val child: ChildType, val parent: ParentType)
                                                      (val f: (ChildType, ParentType) => Unit) extends Callback {
  final override def invoke(): Unit = f(child, parent)
}

/**
  * A type of Callback that is designed to "connect" a child task with any number of parent or upstream
  * tasks.
  */
protected class ManyToOneConnector[ChildType <: Task, ParentType <: Task](val child:ChildType, val parents: List[ParentType])
                                                                         (val f: (ChildType, List[ParentType]) => Unit) extends Callback {
  final override def invoke(): Unit = f(child, parents)
}


