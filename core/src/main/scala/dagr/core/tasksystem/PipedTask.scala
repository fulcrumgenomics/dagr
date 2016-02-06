package dagr.core.tasksystem

import dagr.core.execsystem.ResourceSet

import scala.collection.mutable.ListBuffer

object DataTypes {
  class SamOrBam
  class Sam extends SamOrBam
  class Bam extends SamOrBam
  class Vcf
  class Fastq
}

private object PipeChain {
  /**
    * Builds a new PipeChain from two tasks that support piping.
    *
    * @param left the task on the left hand side of the pipe
    * @param right the task on the right hand side of the pipe
    * @tparam In1  the input type of left hand task, which becomes the input type of the pipeline
    * @tparam Out1 the output type of the left hand task
    * @tparam In2  the input type of the right hand task, which must be a subtype of the left tasks output type
    * @tparam Out2 the output type of the right hand task, which becomes the output type of the pipeline
    * @return a new PipeChain that chains together the two tasks/chains provided
    */
  def apply[In1,Out1,In2 >:Out1,Out2](left: Piping[In1,Out1], right: Piping[In2,Out2]): Piping[In1,Out2] = {
    val xs = ListBuffer[Piping[_,_]]()
    xs.appendAll(left.ordered)
    xs.appendAll(right.ordered)
    new PipeChain(tasks=xs.toList, first=left.left, last=right.right)
  }
}

/**
  * An immutable class representing a chain of tasks that can be piped together.
  */
private class PipeChain[In,Out](val tasks: List[Piping[_,_]], val first: Piping[In,_], val last: Piping[_,Out]) extends ProcessTask with Piping[In,Out] {
  if (tasks.filterNot(_.isInstanceOf[FixedResources]).size > 1) {
    throw new IllegalArgumentException("Pipe chain includes more than one task that does not implement FixedResources.")
  }

  override private[tasksystem] def left: Piping[In, _] = this.first
  override private[tasksystem] def right: Piping[_, Out] = this.last
  override private[tasksystem] def ordered: Seq[Piping[_, _]] = this.tasks

  /** Returns a single Seq over the args of all piped tasks, with pipe operators between them! */
  override def args: Seq[Any] = {
    tasks.foldLeft[List[Any]](Nil)((list, task) => list ++ list.headOption.map(x => "|") ++ task.args)
  }

  /**
    * Splits the tasks into a seq of tasks that implement fixed resources, and optionally one
    * task that does not implement FixedResources.
    */
  private def partition: (Seq[FixedResources], Option[ProcessTask]) = {
    val (fixed, variable) = this.tasks.partition(task => task.isInstanceOf[FixedResources])
    (fixed.map(_.asInstanceOf[FixedResources]), variable.headOption)
  }


  /**
    * Picks resources for the piped set of tasks by attempting to allocate the necessary resources
    * to all the tasks that implement FixedResources, and then offering the rest to the optional
    * non-fixed resource requiring task.
    */
  override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
    val (fixed, variable) = partition
    val fixedResources: ResourceSet = fixed.map(_.resources).foldRight(ResourceSet.empty)((a,b) => a+ b)

    if (availableResources.subset(fixedResources).isDefined) {
      val remaining = availableResources - fixedResources
      variable.flatMap(_.pickResources(remaining)) match {
        case Some(resources) => Some(fixedResources + resources)
        case None            => Some(fixedResources)
      }
    }
    else {
      None
    }
  }

  /**
    * Allocates the necessary resources to the fixed resource tasks, and the rest to a variable
    * resource task if there is one.
    */
  override def applyResources(resources: ResourceSet): Unit = {
    val (fixed, variable) = partition
    var remainingResource = resources
    fixed.foreach(task => { task.applyResources(task.resources); remainingResource -= task.resources })
    variable.foreach(task => task.applyResources(remainingResource))
  }
}

/**
  * Trait for tasks that would like to be able to pipe data using stdin and stdout. Tasks
  * are still responsible for managing their input and output - so if tasks can also read
  * and write from files, they must have the input set to Io.Stdin and/or output to Io.Stdout
  * before connecting them with pipes.
  *
  * See also the traits [[PipeIn]] and [[PipeOut]] which provide a simplified way to implement
  * a piping task that only receives on or only emits to a pipe.
 *

  * @tparam In a symbolic type representing the kind of data the task can accept
  * @tparam Out a symbolic type representing the kind of fdata the task emits
  */
trait Piping[In,Out] extends ProcessTask {
  /** Generates another Piper that is the result of this piper piped into the new piper. */
  def |[In2 >: Out,Out2](next: Piping[In2,Out2]): Piping[In,Out2] = {
    PipeChain(this, next)
  }

  private[tasksystem] def left: Piping[In,_] = this
  private[tasksystem] def right: Piping[_,Out] = this
  private[tasksystem] def ordered: Seq[Piping[_,_]] = List[Piping[_,_]](this)
}

/** A simplified trait for sink tasks that can receive data via a pipe, but cannot pipe onwards. */
trait PipeIn[In] extends Piping[In,Nothing]

/** A simplified trait for generate tasks that can pipe out, but cannot recieve data from a pipe. */
trait PipeOut[Out] extends Piping[Nothing,Out]
