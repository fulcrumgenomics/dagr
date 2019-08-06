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
package dagr.core.cmdline.pipelines

import com.fulcrumgenomics.sopt._
import dagr.core.tasksystem.Pipeline

import com.fulcrumgenomics.sopt.cmdline.ClpGroup

private class TestGroup extends ClpGroup {
  val name: String = "Testing"
  val description: String = "Testing"
}

@clp(description = "", group = classOf[TestGroup], hidden = true)
private[cmdline] class CommandLineTaskTesting extends Pipeline {
  override def build(): Unit = ()
}

@clp(description = "", group = classOf[TestGroup], hidden = true)
private[cmdline] case class PipelineOne
() extends CommandLineTaskTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private[cmdline] case class PipelineTwo
() extends CommandLineTaskTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private[cmdline] case class PipelineThree
(@arg var argument: String) extends CommandLineTaskTesting // argument should be required

@clp(description = "", group = classOf[TestGroup], hidden = true)
private[cmdline] case class PipelineFour
(@arg var argument: String = "default", @arg var flag: Boolean = false) extends CommandLineTaskTesting

@clp(description = "", group = classOf[TestGroup], hidden = true)
private[cmdline] case class PipelineWithMutex
(@arg(mutex = Array("another")) var argument: String, @arg(mutex = Array("argument")) var another: String) extends CommandLineTaskTesting // argument should be required
