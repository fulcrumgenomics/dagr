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

object EitherTask {
  sealed trait Choice
  object Left  extends Choice
  object Right extends Choice

  /**
    * Creates an [[EitherTask]] that wraps `choice` into a function that will be evaluated lazily when the
    * [[EitherTask]] needs to make its choice.
    *
    * @param left   the left task.
    * @param right  the right task.
    * @param choice an expression that returns either Left or Right when evaluated
    */
  def apply(left: Task, right: Task, choice: => Choice): EitherTask = new EitherTask(left, right, () => choice)

  /**
    * Creates an [[EitherTask]] that wraps `choice` into a function that will be lazily evaluated when the
    * [[EitherTask]] needs to make its choice. If `goLeft` evaluates to true the `Left` task will
    * be returned, else the `Right` task.
    *
    * @param left   the left task.
    * @param right  the right task.
    * @param goLeft an expression that returns a Boolean, with true indicating Left and false indicating Right
    */
  def of(left: Task, right: Task, goLeft: => Boolean): EitherTask = new EitherTask(left, right, () => if (goLeft) Left else Right)
}

/** A task that returns either the left or right task based on a deferred choice. The choice function is
  * not evaluated until all dependencies have been met and the `EitherTask` needs to make a decision about
  * which task to return from [[getTasks].
  *
  * @param left the left task.
  * @param right the right task
  * @param choice an expression that returns either Left or Right when invoked
  */
class EitherTask private (private val left: Task, private val right: Task, private val choice: () => EitherTask.Choice) extends Task {
  /** Decides which task to return based on `choice` at execution time. */
  override def getTasks: Traversable[Task] = Seq(if (choice() eq EitherTask.Left) left else right)
}
