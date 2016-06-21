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
  * A trait that declares all of the dependency management operators for Tasks and their friends,
  * to ensure consistency in implementation between the various objects that implement them.
  */
trait Dependable {
  /** Creates a dependency on this dependable, for the provided Task. */
  final def ==> (other: Dependable) : Dependable = {
    addDependent(other)
    new DependencyChain(this, other)
  }

  /** Must be implemented to handle the addition of a dependent. */
  def addDependent(dependent: Dependable) : Unit

  /** Breaks the dependency link between this dependable and the provided Task. */
  def !=> (other: Dependable) : Unit

  /** Returns an object that can be used to manage dependencies that apply to this and the other Dependable. */
  def :: (other: Dependable) : Dependable = new DependencyGroup(this, other)

  /** Abstract method that must be implemented to return all Tasks associated with this Dependable. */
  private[tasksystem] def toTasks: Traversable[Task]
}

/**
  * Represents a link from one Dependable to another, which can have dependencies wired
  * up to it, and will vector the dependencies to the appropriate sub-dependencies.
  */
class DependencyChain(from: Dependable, to: Dependable) extends Dependable {
  override def addDependent(dependent: Dependable): Unit = to ==> dependent

  /** Breaks the dependency link between this dependable and the provided Task. */
  override def !=>(other: Dependable): Unit = to !=> other

  /** Returns all the tasks associated with both the from and to Dependencies. */
  override private[tasksystem] def toTasks: Traversable[Task] = from.toTasks ++ to.toTasks
}

/**
 * Represents a group of Dependable objects such that depedency operations on a
 * DependencyGroup are transmitted to all contained Dependables.
 */
private class DependencyGroup(val a: Dependable, val b: Dependable) extends Dependable {
  override def addDependent(dependent: Dependable): Unit = foreach(_ ==> dependent)

  override private[tasksystem] def toTasks: Traversable[Task] = a.toTasks ++ b.toTasks

  /** Breaks the dependency link between this dependable and the provided Task. */
  override def !=>(other: Dependable): Unit = foreach(_ !=> other)

  /** Applies a method to each member of the DependencyGroup. */
  def foreach(f: Dependable => Unit): Unit = {
    f(a)
    f(b)
  }
}
