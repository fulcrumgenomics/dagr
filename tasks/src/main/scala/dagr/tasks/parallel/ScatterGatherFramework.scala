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

package dagr.tasks.parallel

import dagr.core.tasksystem.{Pipeline, Task}

////////////////////////////////////////////////////////////////////////////////
// Traits and methods that should be implemented to run scatter gather.
////////////////////////////////////////////////////////////////////////////////

/** The scatter gather framework.
  *
  * There are three distinct steps for scatter gather:
  *   (1) A single input is split into multiple sub-inputs.
  *   (2) Each sub-input is transformed into an output.
  *   (3) Each output is combined to produce a final output.  T
  * A single task (see class [[SplitInputTask]]) is responsible for step (1), a set of scatter tasks (see
  * [[ScatterTask]]) are responsible for step (2), and a gather task (see [[GatherTask]]) is responsible for step (3).
  *
  * The public methods are the minimum that need to be implemented to use the scatter gather framework.
  *
  * Override the protected methods to customize how scatter or gather tasks are generated, for example to perform
  * recursive merging (ala merge-sort).  Otherwise, mix in traits that do this for you.
  *
  * Be sure to mix-in the appropriate trait in the task returned by each method.
  *
  * @tparam Domain    the input type to the scatter gather.
  * @tparam SubDomain the input type to each scatter.
  * @tparam Output    the output type of the scatter gather.
  **/
trait ScatterGatherFramework[Domain, SubDomain, Output] extends Pipeline with GatherTask[Output] {

  /** The input to the scatter gather */
  protected def domain: Domain

  /** Generates a task to split the input into multiple inputs for the scatter tasks. */
  protected def splitDomainTask(domain: Domain): SplitInputTask[Domain, SubDomain]

  /** Generates a scatter task from an input meant for a scatter task. */
  protected def scatterTask(subDomain: SubDomain): ScatterTask[SubDomain, Output]

  /** Generates a gather task that gathers an ordered set of outputs */
  protected def gatherTask(outputs: Iterable[Output]): GatherTask[Output]

  /** Generates the scatter tasks.  No dependencies should be set between scatter tasks and either the split input or
    * gather tasks in this method. */
  protected def scatterTasks(subDomains: Iterable[SubDomain]): Iterable[ScatterTask[SubDomain,Output]] = {
    subDomains.map(in => scatterTask(in))
  }

  /** Provides the final task that contains the output of the scatter gather.  In the case that there is only one scatter,
    * this may be a [[ScatterTask]].  Otherwise, this is more often a [[GatherTask]].  In the case of a complicated
    * gather pipeline, say to recursively merge the outputs (ala merge-sort), the gather containing the final output
    * should be returned.
    *
    * The scatter tasks are provided so in the case of a gather pipeline, individual tasks can depend on the scatter
    * tasks. For example, not all gather tasks in a recursive merge (ala merge-sort) wil depend on the scatter tasks.
    *
    * The gather task(s) should have their dependencies set in this method.
    */
  protected def finalOutputTask(inputTasks: Iterable[ScatterTask[SubDomain,Output]]): OutputTask[Output]
}

/** All tasks that split the input into inputs for the scatter tasks should extend this trait.
  *
  * Provides a method that returns the inputs to the scatter tasks.  The method
  * should only be called *after* this task has been run.  This is typically ensured by
  * using a [[dagr.core.tasksystem.Linker]]. */
trait SplitInputTask[Domain, SubDomain] extends Task {
  protected var _subDomains: Option[Iterable[SubDomain]] = None
  final def subDomains: Iterable[SubDomain] = _subDomains.get
}

/** A task that can return a specific output after it has been run. */
trait OutputTask[Output] extends Task {
  def gatheredOutput: Output
}

/** All tasks that perform the scatter step should extend this trait.
  *
  * Provides a method that returns the output of the scatter task.  The method
  * should only be called *after* this task has been run.  This is typically ensured by
  * using a [[dagr.core.tasksystem.Linker]].
  */
trait ScatterTask[SubDomain, Output] extends OutputTask[Output] {
  protected var _gatheredOutput: Option[Output] = None
  final def gatheredOutput: Output = _gatheredOutput.get
}

/** All tasks that perform the gather step should extend this triat.
  *
  * Provides a method that should be implemented by a gather task that returns the output
  * of the gather.  This method should only be called *after* this task has been run. This
  * is typically ensured by using a [[dagr.core.tasksystem.Linker]].
  */
trait GatherTask[Output] extends OutputTask[Output]
