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
package dagr.core.cmdline.parsing.testing.pipelines

import dagr.core.cmdline._
import dagr.core.cmdline.parsing.TestingProgramGroup
import dagr.core.tasksystem.{Pipeline, Task}

@CLP(summary = "", oneLineSummary = "", pipelineGroup = classOf[TestingProgramGroup], omitFromCommandLine = true)
private[cmdline] class CommandLineTaskTesting extends Pipeline {
  override def build(): Unit = Unit
}

@CLP(summary = "", oneLineSummary = "", pipelineGroup = classOf[TestingProgramGroup], omitFromCommandLine = true)
private[cmdline] case class PipelineOne @CLPConstructor
() extends CommandLineTaskTesting

@CLP(summary = "", oneLineSummary = "", pipelineGroup = classOf[TestingProgramGroup], omitFromCommandLine = true)
private[cmdline] case class PipelineTwo @CLPConstructor
() extends CommandLineTaskTesting

@CLP(summary = "", oneLineSummary = "", pipelineGroup = classOf[TestingProgramGroup], omitFromCommandLine = true)
private[cmdline] case class PipelineThree @CLPConstructor
(@Arg var argument: String) extends CommandLineTaskTesting // argument should be required

@CLP(summary = "", oneLineSummary = "", pipelineGroup = classOf[TestingProgramGroup], omitFromCommandLine = true)
private[cmdline] case class PipelineFour @CLPConstructor
(@Arg var argument: String = "default", @Arg var flag: Boolean = false) extends CommandLineTaskTesting