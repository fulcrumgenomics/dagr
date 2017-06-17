/*
 * The MIT License
 *
 * Copyright (c) 2017 Fulcrum Genomics LLC
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

package dagr.core.exec

import dagr.core.UnitSpec

class SimpleTaskCacheTest extends UnitSpec {

  "SimpleTaskCache" should "execute " in {

  }

  // fails if no replay log could be read

  // tests for validation
  // - Check that a task has one definition
  // - Check that a parent and child have only one relationship
  // - Check the parent and child in each relationship have a definition
  // - Check that a task if is found for each status

  // method register
  // - register a root task (first task with parent == child)
  //   - throw an exception if the root task could not be found
  //   - ensure child number and parent code set correctly
  //   - set to execute if it did not succeed
  //   - not set to execute if it did not succeed
  // - unit task
  //   - register a non-unit unit task (task with parent == child, not root)
  //   - throws an exception if a non-root unit task was not previously registered (must contain parent)
  // - when the parent is not the child
  //   - throws an exception if the parent is not known
  //   - throws an exception if a relationship with the parent in the replay log has a child that cannot be found in the
  //     current set of definitions (add more comments)
  //   - executes the children if there are a different # of children
  //   - executes the children if a child has different simple name
  //   - executes a child if it cannot be associated with a child in the replay log
  //     - due to the parent being forced to execute (due to a parent or ancestor) not being found in the replay log
  //     - the definition of the child in the replay is not "equivalent" to the current child's definition (simple name,
  //       parent code, child number)
  //   - throws an exception if the parent and child have multiple relationships

  // read in a log file with the status of execution
  // - have many tasks and definitions, and execute should be set correctly for all

}
