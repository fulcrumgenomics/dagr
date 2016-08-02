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

import dagr.core.execsystem.{Memory, Cores}

/**
  * Object for creating linker tasks that transfer state between other tasks at
  * runtime.
  */
object Linker {
  /**
    * Generates a link between the parent and the child task that will invoke the `link`
    * method, passing it the `parent` and `child`, after the `parent` has successfully
    * executed and before the `child` is executed.
    *
    * @param from  a task that produces some information on which the child depends
    * @param to    a task which needs to receive some information from the parent task
    * @param link  a method that will receive be invoked with a reference to both tasks
    *              at the appropriate time during pipeline execution
    * @tparam From the type of the parent task (usually inferred)
    * @tparam To   the type of the child task (usually inferred)
    * @return      a task that links the parent and child and will invoke the `link` method
    */
  def apply[From <: Task, To <: Task](from: From, to: To)
                                     (link: (From, To) => Unit): UnitTask = {

    val linker = SimpleInJvmTask(link(from, to))
    linker.name = "LinkerFrom" + from.name + "To" + to.name
    linker.requires(Cores.none, Memory.none)
    from ==> linker ==> to
    linker
  }
}
