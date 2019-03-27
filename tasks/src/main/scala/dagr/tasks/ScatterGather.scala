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

package dagr.tasks

import dagr.core.tasksystem.{Pipeline, SimpleInJvmTask, Task}

import scala.collection.mutable.ListBuffer

/**
 * Provides a set of objects and classes for composing Scatter/Gather tasks and pipelines.
 */
object ScatterGather {

  /**
   * A type of Task that, when run, partitions some kind of input, and produces one or more outputs
   * of type Result that should be parallelized (scattered) over.
   */
  trait Partitioner[Result] extends Task {
    def partitions: Option[Seq[Result]]
  }

  /** Provides a factory method to create a [[Scatter]] object from a [[Partitioner]]. */
  object Scatter {
    def apply[Result](partitioner: Partitioner[Result]): Scatter[Result] = new PartitionerWrapper(partitioner)
  }


  /**
   * One of the two main types that implement Scatter/Gather, Scatter provides methods
   * for mapping the currently scattered type to a new type by running a task in parallel
   * on each of the scattered parts.
   */
  sealed trait Scatter[Result] extends Task {
    /**
     * Produces a new [[Scatter]] from the existing scatter. Takes a function that maps an individual
     * object of type Result to a [[Task]] of type NextResult.  During scatter/gather operation this function will
     * be invoked with each A, to manufacture tasks of type NextResult.
     *
     * The resulting Scatter[B] can be further mapped or gathered (or both!).
     */
    def map[NextResult <: Task](f: Result => NextResult) : Scatter[NextResult]

    /**
     * Produces a [[Gather]] object which will be used at runtime to manufacture a [[Task]] of type NextResult
     * which can gather things of type Result.
     */
    def gather[NextResult <: Task](f: Seq[Result] => NextResult) : Gather[Result,NextResult]

    /**
      * Produces a new [[Scatter]] from the existing scatter. Takes a function that maps an individual
      * object of type Result to a key of type K to partition the task of type Result.  Then takes a function
      * to manufacture a [[Task]] of type NextResult from the given key of type K and partition of objects of type Result.
      *
      * The resulting Scatter[B] can be further mapped or gathered (or both!).
      */
    def groupBy[Key](f: Result => Key) : Scatter[(Key, Seq[Result])]
  }


  /**
   * Represents a gather operation in a scatter/gather pipeline.
   */
  sealed trait Gather[Source, Result <: Task] extends Task {
    private[ScatterGather] var sources: Option[Seq[Source]] = None
  }

  /** Implementation of a Gather that performs a single All->One gather operation. */
  private class SingleGather[Source, Result <: Task](f: Seq[Source] => Result) extends Gather[Source,Result] {
    override def getTasks: Traversable[_ <: Task] = sources match {
      case None    => throw new IllegalStateException("Gather.getTasks called before sources populated.")
      case Some(a) => Some(f(a))
    }
  }


  /**
  * Implementation of a Scatter that is just a thinly veiled wrapper around the Scatterer
  * being used to generate the set of scatters/partitions to operate on.
  */
  private class PartitionerWrapper[Result](partitioner: Partitioner[Result]) extends Scatter[Result] {
    override def getTasks: Traversable[_ <: Task] = Some(partitioner)

    override def gather[NextResult <: Task](f: Seq[Result] => NextResult): Gather[Result,NextResult] =
      throw new UnsupportedOperationException("gather not supported on an unmapped Scatter")

    override def groupBy[Key](f: Result => Key) : Scatter[(Key, Seq[Result])] =
      throw new UnsupportedOperationException("groupBy not supported on an unmapped Scatter")

    override def map[NextResult <: Task](f: Result => NextResult): Scatter[NextResult] = {
      val sub = new PrimarySubScatter(partitioner, f)
      this ==> sub
      sub
    }
  }

  /**
    * Sub-trait of scatter that exists solely because we need two sub-types that share some common functionality.
    * Provides the tracking of tasks generated by the Scatter, sub-Scatters that need to be invoked and wired in
    * when the partitions are available and the set of Gathers to be performed on this stage of the Scatter.
    */
  private trait SubScatter[Result <: Task] extends Scatter[Result] {
    val tasks    = new ListBuffer[Result]
    val subs     = new ListBuffer[SecondarySubScatter[Result, _ <: Task]]
    val gathers  = new ListBuffer[Gather[Result, _ <: Task]]
    val groupers = new ListBuffer[GroupByPartitioner[Result, _]]

    /** Ensures that gathers are wired in correctly. */
    protected def connect(): Unit = {
      val taskList = this.tasks.toList

      gathers.foreach(gather => {
        gather.sources = Some(taskList)
        taskList.foreach(task => task ==> gather)
      })

      groupers.foreach(grouper => {
        grouper.sources = Some(taskList)
        taskList.foreach(task => task ==> grouper.scatter)
      })

      subs.foreach(_.connect())
    }

    override def map[NextResult <: Task](f: Result => NextResult): Scatter[NextResult] = {
      val sub = new SecondarySubScatter[Result,NextResult](f)
      this.subs += sub
      sub
    }

    override def gather[NextResult <: Task](f: Seq[Result] => NextResult): Gather[Result,NextResult] = {
      val gather = new SingleGather[Result,NextResult](f)
      this.gathers += gather
      gather
    }

    override def groupBy[Key](f: Result => Key) : Scatter[(Key, Seq[Result])] = {
      val grouper = new GroupByPartitioner[Result,Key](f)
      this.groupers += grouper
      grouper.scatter
    }
  }

  /**
  * A type of Scatter that is used to represent the first round of Scatter operation after the
  * partitioner has created the partitions.  It's tasks are generated using the scatter partitions
  * as input, and is also responsible for chaining together sub-scatters.
  */
  private class PrimarySubScatter[Source, Result <: Task](private val partitioner: Partitioner[Source], f: Source => Result) extends Pipeline with SubScatter[Result] {
    override def build(): Unit = {
      val as: Seq[Source] = partitioner.partitions match {
        case Some(seq) => seq
        case None      => throw new IllegalStateException("Scatter/Gather pipeline trying to build before scatters populated.")
      }

      // Create the list of tasks and chain of sub-scatterers
      as.foreach { scatter: Source =>
        val task: Result = f(scatter)
        this.tasks += task
        root ==> task
        subs.foreach(sub => task ==> sub.chain(task))
      }

      connect()
    }
  }

  /**
  * A type of scatter that is found at stage 2 and beyond where the input is guaranteed to
  * be a Task already, and not a partition on some domain type (e.g. intervals).
  */
  private class SecondarySubScatter[Source <: Task, Result <: Task](f: Source => Result) extends SubScatter[Result] {
    def chain(a: Source): Result = {
      val task: Result = f(a)
      this.tasks += task
      this.tasksDependingOnThisTask.foreach(other => task ==> other)
      this.tasksDependedOn.foreach(other => other ==> task)
      subs.foreach(sub => task ==> sub.chain(task))
      task
    }

    override def getTasks: Traversable[_ <: Task] = {
      throw new UnsupportedOperationException("getTasks is not supported and should never be called on sub-scatters.")
    }
  }

  /**
    * A type of partitioner that is found at stage 2 and beyond where the input is guaranteed to be a task already, and
    * not part of some domain type.  The given method `f` is used to group the source tasks.
    */
  private class GroupByPartitioner[Result <: Task, Key](f: Result => Key) extends SimpleInJvmTask with Partitioner[(Key, Seq[Result])] {
    var _partitions: Option[Seq[(Key, Seq[Result])]] = None
    var sources: Option[Seq[Result]] = None
    val scatter: Scatter[(Key, Seq[Result])] = Scatter(this)
    override def partitions: Option[Seq[(Key, Seq[Result])]] = this._partitions
    override def run(): Unit = this.sources match {
      case None => throw new IllegalStateException("GroupByPartitioner.tasks called before _tasks populated.")
      case Some(_tasks) => this._partitions = Some(_tasks.groupBy(f).toSeq)
    }
  }
}
