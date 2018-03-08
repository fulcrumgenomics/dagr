/*
 * The MIT License
 *
 * Copyright (c) 2015-6 Fulcrum Genomics LLC
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

import java.nio.file.Path

import com.fulcrumgenomics.commons.io.Io
import dagr.core.tasksystem.Pipes.{RedirectErrorToFile, RedirectToFile}
import com.fulcrumgenomics.commons.CommonsDef.unreachable
import dagr.api.models.util.{Cores, Memory, ResourceSet}

///////////////////////////////////////////////////////////////////////////////
// Section: General traits related to piping
///////////////////////////////////////////////////////////////////////////////

/** Phantom type that is used to represent the situation where nothing comes out of a pipe. */
private[tasksystem] sealed class Void

/**
  * Trait for tasks that would like to be able to pipe data using stdin and stdout. Tasks
  * are still responsible for managing their input and output - so if tasks can also read
  * and write from files, they must have the input set to `Io.Stdin` and/or output to `Io.Stdout`
  * before connecting them with pipes.
  *
  * See also the traits [[PipeIn]] and [[PipeOut]] which provide a simplified way to implement
  * a piping task that only receives on or only emits to a pipe.
  *
  * @tparam In a symbolic type representing the kind of data the task can accept
  * @tparam Out a symbolic type representing the kind of data the task emits
  */
trait Pipe[In,Out] extends ProcessTask {
  /**
    * Generates or extends a pipe chain by connecting this task to the next task via a pipeline.
    *
    * @param next a [[Pipe]] enabled task that should receive this pipe's output as its input.
    * @tparam In2 the type of input expected by the next task. Must be the same or a supertype
    *             of this pipe's output.
    * @tparam Out2 the type of output of the next task, which becomes the output type of the
    *              returned pipe chain.
    * @return a new [[Pipe]] object that is the result of connecting the two pipes.
    */
  def |[In2 >: Out,Out2](next: Pipe[In2,Out2]): Pipe[In,Out2] = {
    Pipes.chain(this, next)
  }

  /** Terminate a pipe chain by writing the output to a file. */
  def >(path: Path): Pipe[In,Void] = {
    Pipes.chain(this, new RedirectToFile[Out](path, append=false))
  }

  /** Terminates a pipe chain by appending the output to a file. */
  def >>(path: Path): Pipe[In,Void] = {
    Pipes.chain(this, new RedirectToFile[Out](path, append=true))
  }

  /** Writes the standard error stream to the given path. */
  def >!(path:Path): Pipe[In,Out] = {
    Pipes.chain(this, new RedirectErrorToFile[Out, Out](path, append=false))
  }

  /** Appends the standard error stream to the given path. */
  def >>!(path:Path): Pipe[In,Out] = {
    Pipes.chain(this, new RedirectErrorToFile[Out, Out](path, append = true))
  }

  /** Discards the standard error stream. */
  def discardError(): Pipe[In,Out] = >!(Io.DevNull)

  /** Returns the first task in the pipe. */
  private[tasksystem] def left: Pipe[In,_] = this
  /** Returns the last task in the pipe. */
  private[tasksystem] def right: Pipe[_,Out] = this
  /** Returns the ordered set of Pipes/tasks in the chain from first to last inclusive. */
  private[tasksystem] def ordered: Seq[Pipe[_,_]] = Seq[Pipe[_,_]](this)
}

/** A simplified trait for sink tasks that can receive data via a pipe, but cannot pipe onwards. */
trait PipeIn[In] extends Pipe[In,Void]

/** A simplified trait for generate tasks that can pipe out, but cannot receive data from a pipe. */
trait PipeOut[Out] extends Pipe[Void,Out]

///////////////////////////////////////////////////////////////////////////////
// Section: Objects and private utility classes to make it all work
///////////////////////////////////////////////////////////////////////////////
object Pipes {
  /** The string that is sandwiched between commands to form pipes. */
  val PipeString                   = " \\\n    | "
  val RedirectAppendString         = " \\\n    >> "
  val RedirectOverwriteString      = " \\\n    > "
  val RedirectErrorAppendString    = " \\\n    2>> "
  val RedirectErrorOverwriteString = " \\\n    2> "

  /** A trait for pipe tasks that consumes no cores and no memory */
  trait PipeWithNoResources[In,Out] extends Pipe[In,Out] with FixedResources {
    val memory: Memory = Memory.none
    val cores: Cores = Cores.none
    this.requires(cores, memory)
  }

  /**
    * A class that represents an empty pipe, that allows for easier construction of pipes with conditional branches.
    * Must remain private and only be instantiated by [[Pipes.empty]].
    */
  private class EmptyPipe[T] extends Pipe[T,T] {
    override def args: Seq[Any] = unreachable("Should not reach here.")
    override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = unreachable("Should not reach here.")
  }

  /**
    * A class that allows redirecting to a file at the end of a Pipe.
    */
  private[tasksystem] class RedirectToFile[In](val path: Path, val append: Boolean) extends PipeIn[In] with FixedResources {
    requires(Cores.none, Memory.none)
    override def args: Seq[Any] = unreachable("Should not reach here.")
    /* Returns the appropriate redirect symbol for appending or overwriting. */
    def redirect: String = if (append) RedirectAppendString else RedirectOverwriteString
  }

  /**
    * A class that allows redirecting the error stream to a file at the end of any pipe.
    */
  private[tasksystem] class RedirectErrorToFile[In,Out](val path: Path, val append: Boolean) extends Pipe[In,Out] with FixedResources {
    requires(Cores.none, Memory.none)
    override def args: Seq[Any] = unreachable("Should not reach here.")
    /* Returns the appropriate redirect symbol for appending or overwriting. */
    def redirect: String = if (append) RedirectErrorAppendString else RedirectErrorOverwriteString
  }
  /**
    * An immutable class representing a chain of tasks that can be piped together. Currently only supports chains
    * with up to a maximum of one non-[[FixedResources]] task, in order to keep the resource management code simple.
    *
    * The constructor does not check types as thoroughly as one might like, instead this is done in
    * [[Pipes.chain()]], which is the only place the constructor is called from.
    */
  private class PipeChain[In,Out](val tasks: Seq[Pipe[_,_]], val first: Pipe[In,_], val last: Pipe[_,Out]) extends ProcessTask with Pipe[In,Out] {
    if (tasks.filterNot(_.isInstanceOf[FixedResources]).size > 1) {
      throw new IllegalArgumentException("Pipe chain includes more than one task that does not implement FixedResources.")
    }

    override private[tasksystem] def left: Pipe[In, _] = this.first
    override private[tasksystem] def right: Pipe[_, Out] = this.last
    override private[tasksystem] def ordered: Seq[Pipe[_, _]] = this.tasks

    /**
      * Overridden to provide a command that pipes together each of the sub-commands in the pipe.
      */
    override private[core] def commandLine: String = tasks match {
      case task :: Nil =>
        task.commandLine
      case firstTask :: subsequentTasks  =>
        subsequentTasks.foldLeft(firstTask.commandLine) {
          case (cmd, next: RedirectToFile[_]) => cmd + next.redirect + next.path.toString
          case (cmd, next: RedirectErrorToFile[_,_]) => cmd + next.redirect + next.path.toString
          case (cmd, next: Pipe[_,_])         => cmd + PipeString + next.commandLine
        }
    }

    /** Overridden to enforce that it is never called. */
    override def args: Seq[Any] = {
      throw new NotImplementedError("args should never be called on PipeChain, only commandLine().")
    }

    /**
      * Splits the tasks into a seq of tasks that implement fixed resources, and optionally one
      * task that does not implement [[FixedResources]].
      */
    private def partition: (Seq[FixedResources], Option[ProcessTask]) = {
      val (fixed, variable) = this.tasks.partition(task => task.isInstanceOf[FixedResources])
      (fixed.map(_.asInstanceOf[FixedResources]), variable.headOption)
    }


    /**
      * Picks resources for the piped set of tasks by attempting to allocate the necessary resources
      * to all the tasks that implement [[FixedResources]], and then offering the rest to the optional
      * non-fixed resource requiring task.
      */
    override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
      val (fixedTasks, variableTask) = partition
      val fixedResources: ResourceSet = fixedTasks.map(_.resources).foldRight(ResourceSet.empty)((a, b) => a + b)

      variableTask match {
        case None    => availableResources.subset(fixedResources)
        case Some(v) => availableResources.minusOption(fixedResources) flatMap { remaining =>
          v.pickResources(remaining) map (_ + fixedResources)
        }
      }
    }

    /**
      * Allocates the necessary resources to the fixed resource tasks, and the rest to a variable
      * resource task if there is one.
      */
    override def applyResources(resources: ResourceSet): Unit = {
      val (fixed, variable) = partition
      val remainingResource = fixed.map(task => { task.applyResources(task.resources); task.resources }).fold(resources)(_ - _)
      variable.foreach(task => task.applyResources(remainingResource))
    }
  }

  /** Returns an "empty" pipe that can be pushed into a pipe chain and does nothing. */
  def empty[T]: Pipe[T,T] = new EmptyPipe[T]

  /**
    * Builds a new [[PipeChain]] from two tasks that support piping.
    *
    * @param left  the task on the left hand side of the pipe
    * @param right the task on the right hand side of the pipe
    * @tparam In1  the input type of left hand task, which becomes the input type of the pipeline
    * @tparam Out1 the output type of the left hand task
    * @tparam In2  the input type of the right hand task, which must be a subtype of the left tasks output type
    * @tparam Out2 the output type of the right hand task, which becomes the output type of the pipeline
    * @return a new PipeChain that chains together the two tasks/chains provided
    */
  private[tasksystem] def chain[In1,Out1,In2 >:Out1,Out2](left: Pipe[In1,Out1], right: Pipe[In2,Out2]): Pipe[In1,Out2] = {
    (left, right) match {
      case (p: EmptyPipe[_], _) => right.asInstanceOf[Pipe[In1,Out2]]
      case (_, p: EmptyPipe[_]) => left.asInstanceOf[Pipe[In1,Out2]]
      case (_, _)               => new PipeChain(tasks=left.ordered ++ right.ordered, first=left.left, last=right.right)
    }
  }
}
