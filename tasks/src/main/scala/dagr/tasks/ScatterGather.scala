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
    /** The resulting partitions after this task has been run */
    def partitions: Option[Seq[Result]]
    /** A [[Scatter]] over the resulting partitions. */
    private[tasks] val scatter: Scatter[Result] = new PartitionerWrapper[Result](this)
  }

  /**
    * A type of Task that, when run, re-partitions some kind of input, and produces one or more outputs
    * of type Result that should be parallelized (scattered) over.
    */
  trait RePartitioner[Source <: Task, Result] extends Partitioner[Result] {
    /** The [[Source]] tasks that this task will re-partition and transform into one or more [[Result]]s. */
    def sources: Option[Seq[Source]]
  }

  /** Provides a factory method to create a [[Scatter]] object from a [[Partitioner]]. */
  object Scatter {
    def apply[Result](partitioner: Partitioner[Result]): Scatter[Result] = partitioner.scatter
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
     * be invoked with each Result, to manufacture tasks of type NextResult.
     *
     * The resulting Scatter[NextResult] can be further mapped or gathered (or both!).
     */
    def map[NextResult <: Task](f: Result => NextResult) : Scatter[NextResult]

    /**
      * Produces a new [[Scatter]] from the existing scatter. Takes a function that maps an individual
      * object of type Result to one or more [[Task]]s of type NextResult.  During scatter/gather operation
      * this function will be invoked with each Result, to manufacture tasks of type NextResult.
      *
      * The manufactured tasks of type NextResult will be flattened such that they can be further individually mapped or
      * gathered (or both!).
      */
    def flatMap[NextResult](f: Result => Scatter[NextResult]) : Scatter[NextResult]

    /**
     * Produces a [[Gather]] object which will be used at runtime to manufacture a [[Task]] of type NextResult
     * which can gather things of type Result.
     */
    def gather[NextResult <: Task](f: Seq[Result] => NextResult) : Gather[Result,NextResult]

    /**
      * Produces a new [[Scatter]] from the existing scatter.  Partitions the the scattered objects of type Result
      * according to the given discriminator function.  The discriminator function maps an individual object of type
      * Result to a key of type Key.  The partitions are each a sequence of objects of type Result that share the same key
      * of type Key when the discriminator function is applied.
      *
      * The resulting Scatter[(Key, Seq[Result])] can be further mapped or gathered (or both!).
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
    override def getTasks: Iterable[_ <: Task] = sources match {
      case None    => throw new IllegalStateException("Gather.getTasks called before sources populated.")
      case Some(a) => Some(f(a))
    }
  }

  /** Implementation of a Partitioner that takes the partitions from an existing Partitioner, and then groups them. */
  private class GroupByPartitioner[Result, Key](partitioner: Partitioner[Result], f: Result => Key) extends SimpleInJvmTask with Partitioner[(Key, Seq[Result])] {
    var partitions: Option[Seq[(Key, Seq[Result])]] = None
    override def run(): Unit = {
      partitioner.partitions match {
        case None             => throw new IllegalStateException(s"partitioner.partitions called before partitions populated.")
        case Some(_partition) => this.partitions = Some(_partition.groupBy(f).toSeq)
      }
    }
  }

  /**
  * Implementation of a Scatter that is just a thinly veiled wrapper around the Scatterer
  * being used to generate the set of scatters/partitions to operate on.
  */
  private class PartitionerWrapper[Result](val partitioner: Partitioner[Result]) extends Scatter[Result] {

    override def getTasks: Iterable[_ <: Task] = Some(partitioner)

    override def gather[NextResult <: Task](f: Seq[Result] => NextResult): Gather[Result,NextResult] =
      throw new UnsupportedOperationException("gather not supported on an unmapped Scatter")

    override def groupBy[Key](f: Result => Key) : Scatter[(Key, Seq[Result])] = {
      val grouper = new GroupByPartitioner[Result, Key](partitioner, f)
      this ==> grouper.scatter
      grouper.scatter
    }

    override def flatMap[NextResult](f: Result => Scatter[NextResult]) : Scatter[NextResult] = {
      this.map(f).flatMap(identity)
    }

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
    // the resulting tasks generated by the scatter
    val tasks    = new ListBuffer[Result]
    // any sub-scatterers that further map the results
    val subs     = new ListBuffer[SecondarySubScatter[Result, _ <: Task]]
    // any gatherers that gather the results
    val gathers  = new ListBuffer[Gather[Result, _ <: Task]]
    // any tasks that reduces the results (i.e. flattens or further groups)
    val reducers = new ListBuffer[SecondaryRePartitioner[Result, _]]

    /** Ensures that gathers are wired in correctly. */
    protected def connect(): Unit = {
      val taskList = this.tasks.toList

      gathers.foreach { gather =>
        gather.sources = Some(taskList)
        taskList.foreach { task => task ==> gather }
      }

      reducers.foreach { partitioner =>
        partitioner.sources = Some(taskList)
        taskList.foreach { task => task ==> partitioner.scatter }
      }

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
      val grouper = new GroupByRePartitioner[Result,Key](f)
      this.reducers += grouper
      grouper.scatter
    }

    override def flatMap[NextResult](f: Result => Scatter[NextResult]): Scatter[NextResult] = {
      val flatener = new FlatMapRePartitioner[Result, NextResult](f)
      this.reducers += flatener
      flatener.scatter
    }
  }

  /**
    * A type of Scatter that is used to represent the first round of Scatter operation after the
    * [[Partitioner]] has created the partitions.  It's tasks are generated using the scatter partitions
    * as input, and is also responsible for chaining together sub-scatters.
    */
  private class PrimarySubScatter[Source, Result <: Task](partitioner: Partitioner[Source], f: Source => Result) extends Pipeline with SubScatter[Result] {
    override def build(): Unit = {
      val sources: Seq[Source] = partitioner.partitions.getOrElse {
        throw new IllegalStateException("Scatter/Gather pipeline trying to build before scatters populated.")
      }

      // Create the list of tasks and chain of sub-scatterers
      sources.map(f).foreach { task: Result =>
        this.tasks += task
        root ==> task
        subs.foreach { sub => sub.chain(task).foreach { s => task ==> s } }
      }

      connect()
    }
  }

  /**
    * A type of scatter that is found at stage 2 and beyond where the input is guaranteed to
    * be a Task already, and not a partition on some domain type (e.g. intervals).
    */
  private class SecondarySubScatter[Source <: Task, Result <: Task](f: Source => Result) extends SubScatter[Result]{
    def chain(source: Source): Seq[Result] = {
      val task: Result = f(source)
      this.tasks += task
      this.tasksDependingOnThisTask.foreach(other => task ==> other)
      this.tasksDependedOn.foreach(other => other ==> task)
      subs.foreach { sub => sub.chain(task).foreach { s => task ==> s } }
      Seq(task)
    }
    override def getTasks: Iterable[_ <: Task] =
      throw new UnsupportedOperationException("getTasks is not supported and should never be called on sub-scatters.")
  }

  /**
    * A type of [[Partitioner]] that is found at stage 2 and beyond where the input is guaranteed to be a task already, and
    * not part of some domain type.  Will take a in sequence of source tasks, and produce a sequence of results over
    * which a new scatter can be performed.
    */
  private trait SecondaryRePartitioner[Source <: Task, Result] extends SimpleInJvmTask with RePartitioner[Source, Result] {
    var partitions: Option[Seq[Result]] = None
    var sources: Option[Seq[Source]] = None
    /** The method to repartition the sequence of [[Source]]s to the sequences of [[Result]]s. */
    protected def repartition(sources: Seq[Source]): Seq[Result]
    override def run(): Unit = {
      this.sources match {
        case None => throw new IllegalStateException(s"${getClass.getSimpleName}.sources called before _tasks populated.")
        case Some(_sources) => this.partitions = Some(repartition(_sources))
      }
    }
  }

  /** A [[RePartitioner]] whose partitions are the concatenation of the results of one or more [[Scatter]]s. The
    * given method will ensure that the sources can be converted to [[Scatter]]s. */
  private class FlatMapRePartitioner[Source <: Task, Result](toScatter: Source => Scatter[Result]) extends SecondaryRePartitioner[Source, Result] {
    protected def repartition(sources: Seq[Source]): Seq[Result] = sources.map(toScatter).flatMap {
      case _scatter: SubScatter[Result]        => _scatter.tasks
      case wrapper: PartitionerWrapper[Result] =>
        wrapper.partitioner.partitions.getOrElse {
          throw new IllegalStateException("Scatter/Gather pipeline trying to build before scatters populated.")
        }
      case _scatter => throw new IllegalStateException(s"Unknown scatter: ${_scatter.getClass.getSimpleName}")
    }
  }

  /** A [[RePartitioner]] whose partitions are groups of sources grouped by the given method `f`. */
  private class GroupByRePartitioner[Result <: Task, Key](f: Result => Key) extends SecondaryRePartitioner[Result, (Key, Seq[Result])] {
    protected def repartition(sources: Seq[Result]): Seq[(Key, Seq[Result])] = sources.groupBy(f).toSeq
  }
}
