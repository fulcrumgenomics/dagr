/*
 * The MIT License
 *
 * Copyright (c) $year Fulcrum Genomics
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

package dagr.tasks.parallel

import dagr.core.tasksystem._

////////////////////////////////////////////////////////////////////////////////
// Scatter gather pipelines.
////////////////////////////////////////////////////////////////////////////////

/** Useful traits and classes for implementing scatter gather pipelines. */
object ScatterGatherPipeline {
  /** Delegates the return of the output of a given task.  The `output` method should only be called *after* this
    * task has been run. This is typically ensured by using a [[Linker]].*/
  trait GatherOutputTaskDelegator[Output] extends OutputTask[Output] {
    var gatherOutputTask: Option[OutputTask[Output]] = None
    override def gatheredOutput: Output = gatherOutputTask.get.gatheredOutput
  }

  /** A task that creates the gather task when getTasks is called so that we guarantee that the inputs are populated
    * in each input task prior to gather task creation.  We also facilitate the return of the gather task's output.
    * This task is set to be dependent on all input tasks during the construction of this task. */
  class LazyGatherOutputTaskDelegator[Output]
  (
    val inputTasks         : Iterable[OutputTask[Output]],
    val generateGatherTask : Iterable[Output] => GatherTask[Output]
  ) extends GatherOutputTaskDelegator[Output] {

    inputTasks.foreach(_ ==> this)

    override def getTasks: Traversable[_ <: Task] = {
      gatherOutputTask = Some(generateGatherTask(inputTasks.map(_.gatheredOutput)))
      List(gatherOutputTask.get)
    }
  }

  /** Companion object to ScatterGatherFrameworkAdaptor that holds classes to help implement the adaptor */
  object ScatterGatherPipelineAdaptor {
    abstract class SimpleSplitInputTask[Domain, SubDomain]
      extends SimpleInJvmTask
      with SplitInputTask[Domain, SubDomain]

    abstract class SimpleScatterTask[SubDomain, Output]
      extends SimpleInJvmTask
      with ScatterTask[SubDomain, Output]

    abstract class SimpleGatherTask[Output]
      extends SimpleInJvmTask
      with GatherTask[Output]
  }

  /** A class to create a [[ScatterGatherPipeline]] from supplied methods.  This will create tasks to wrap and
    * execute each method in the correct order. This also allows other traits to mixed in as well. */
  class ScatterGatherPipelineAdaptor[Domain, SubDomain, Output]
  (
    inDomain: Domain,
    toSubDomains: Domain => Iterable[SubDomain],
    toOutput: SubDomain => Output,
    toFinalOutput: Iterable[Output] => Output
  ) extends ScatterGatherPipeline[Domain, SubDomain, Output] {
    import ScatterGatherPipelineAdaptor._
    def domain: Domain = inDomain
    def splitDomainTask(domain: Domain) = new SimpleSplitInputTask[Domain, SubDomain] {
      override def run(): Unit = _subDomains = Option(toSubDomains(domain))
    }
    def scatterTask(subDomain: SubDomain): ScatterTask[SubDomain, Output] =
      new SimpleScatterTask[SubDomain, Output] {
        override def run(): Unit = _gatheredOutput = Option(toOutput(subDomain))
      }
    def gatherTask(outputs: Iterable[Output]): GatherTask[Output] = new SimpleGatherTask[Output] {
      private var _output: Option[Output] = None
      override def gatheredOutput: Output = _output.get
      override def run(): Unit = _output = Option(toFinalOutput(outputs))
    }
  }

  /** Creates a ScatterGatherPipeline from the supplied methods */
  def apply[Domain, SubDomain, Output]
  (
    inDomain: Domain,
    toSubDomains: Domain => Iterable[SubDomain],
    toOutput: SubDomain => Output,
    toFinalOutput: Iterable[Output] => Output
  ): ScatterGatherFramework[Domain, SubDomain, Output] = {
    new ScatterGatherPipelineAdaptor[Domain, SubDomain, Output](
      inDomain=inDomain,
      toSubDomains=toSubDomains,
      toOutput=toOutput,
      toFinalOutput=toFinalOutput
    )
  }
}

/** The base trait for scatter gather pipelines.  This trait wires together the various methods in the
  * scatter gather framework.  It also implements a simple single gather step to gather all the outputs of
  * the scatter steps.
  *
  * @tparam Domain        the input type to the scatter gather.
  * @tparam SubDomain the input type to each scatter.
  * @tparam Output       the output type of hte scatter gather.
  */
trait ScatterGatherPipeline[Domain, SubDomain, Output]
  extends ScatterGatherFramework[Domain, SubDomain, Output] {
  import ScatterGatherPipeline.{GatherOutputTaskDelegator, LazyGatherOutputTaskDelegator}

  /** The gather pipeline, which will return the final output. */
  private var gatherPipeline: Option[GatherOutputPipeline] = None

  /** The output of the final gather task.  This method should only be called after the this pipeline has been run.
    * Use a [[Linker]] if you wish to have another task depend on this value. */
  override final def gatheredOutput: Output = gatherPipeline.get.gatheredOutput

  override final def build(): Unit = {
    val inputTask = splitDomainTask(domain)
    gatherPipeline = Some(new GatherOutputPipeline)
    root ==> inputTask ==> gatherPipeline.get
    // set the intermediate inputs for the scatter steps to be updated after the input has been split. */
    Linker(from=inputTask, to=gatherPipeline.get)((from, to) => to.subDomains = from.subDomains)
  }

  /** Simple pipeline to wrap the creation of the scatter and gather tasks. This should depend on the `splitInputTask`
    * since the inputs to the scatter steps will not be available until the `splitInputTask` task completes. */
  private class GatherOutputPipeline extends Pipeline with GatherOutputTaskDelegator[Output] {
    /** The intermediate inputs are the inputs to the scatter steps.  A callback should be create to set these. */
    var subDomains: Iterable[SubDomain] = Nil
    override def build(): Unit = {
      val tasks = scatterTasks(subDomains)
      tasks.foreach(root ==> _)
      gatherOutputTask = Some(finalOutputTask(tasks))
    }
  }

  /** Generates a single gather step that gathers all the scatters. */
  override protected def finalOutputTask(inputTasks: Iterable[ScatterTask[SubDomain,Output]]): OutputTask[Output] = {
    // do not *create* the gather step until the scatter steps have completed.
    val task = new LazyGatherOutputTaskDelegator[Output](
      inputTasks=inputTasks,
      generateGatherTask=gatherTask
    )
    task
  }
}

/** Companion to MergingScatterGatherPipeline */
object MergingScatterGatherPipeline {
  import ScatterGatherPipeline._

  val DefaultMergeSize = 2

  /** Creates a MergingScatterGatherPipeline from the supplied methods */
  def apply[Domain, SubDomain, Output]
  (
    inDomain: Domain,
    toSubDomains: Domain => Iterable[SubDomain],
    toOutput: SubDomain => Output,
    toFinalOutput: Iterable[Output] => Output
  ): MergingScatterGatherPipeline[Domain, SubDomain, Output] = {
    new ScatterGatherPipelineAdaptor[Domain, SubDomain, Output](
      inDomain=inDomain,
      toSubDomains=toSubDomains,
      toOutput=toOutput,
      toFinalOutput=toFinalOutput
    ) with MergingScatterGatherPipeline[Domain, SubDomain, Output]
  }
}

/** A scatter-gather pipeline that will perform a recursive merge gather (ala merge-sort).  Please note that
  * `gatherTask` must be able to be called multiple times for this to work.
  * */
trait MergingScatterGatherPipeline[Domain, SubDomain, Output]
  extends ScatterGatherPipeline[Domain, SubDomain, Output] {
  import ScatterGatherPipeline.LazyGatherOutputTaskDelegator

  def mergeSize: Int = MergingScatterGatherPipeline.DefaultMergeSize

  /** Performs a recursive merge of the gather steps, grouping scatter steps into sub-sets based on the merge size,
    * merging each sub-set, and iterating until there is only one step. */
  override protected def finalOutputTask(inputTasks: Iterable[ScatterTask[SubDomain, Output]]): OutputTask[Output] = {
    if (inputTasks.size == 1) {
      inputTasks.head
    }
    else {
      var currentTasks: Iterable[OutputTask[Output]] = inputTasks
      while (1 < currentTasks.size) {
        currentTasks = currentTasks.sliding(mergeSize, mergeSize).map {
          tasks =>
            if (tasks.size > 1) {
              new LazyGatherOutputTaskDelegator(inputTasks=tasks, generateGatherTask=gatherTask)
            }
            else {
              tasks.head
            }
        }.toList
      }
      currentTasks.head
    }
  }
}
