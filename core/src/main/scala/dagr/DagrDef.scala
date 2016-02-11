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
package dagr

/**
  * Object that is designed to be imported with `import DagrDef._` in any/all dagr classes
  * much like the way that scala.PreDef is imported in all files automatically.
  *
  * New methods, types and objects should not be added to this class lightly as they
  * will pollute the namespace of any classes which import it.
  */
object DagrDef {
  /** An exception that implies that code is unreachable. */
  private class UnreachableException(message: String) extends IllegalStateException(message)

  /**
    * A terse way to throw an `UnreachableException` that can be used where any type is expected,
    * e.g. `Option(thing) getOrElse unreachable("my thing is never null")`
    *
    * @param message an optional message
    */
  def unreachable(message: => String = ""): Nothing = throw new UnreachableException(message)


  /**
    * Construct to capture a value, execute some code, and then returned the captured value. Allows
    * code like:
    *   `val x = foo; foo +=1; return x`
    * to be replaced with
    *   `yieldAndThen(foo) {foo +=1}`
    * @param it the value to be returned/yielded
    * @param block a block of code to be evaluated
    * @tparam A the type of thing to be returned (usually inferred)
    * @return it
    */
  def yieldAndThen[A](it: => A)(block: => Unit): A = {
    val retval : A = it
    block
    retval
  }

  /** The type of identifier used to uniquely identify tasks tracked by the execution system. */
  type TaskId = BigInt
}
