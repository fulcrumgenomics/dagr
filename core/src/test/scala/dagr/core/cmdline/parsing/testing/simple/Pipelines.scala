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
package dagr.core.cmdline.parsing.testing.simple

import dagr.core.cmdline._
import dagr.core.tasksystem.Pipeline

/** For testing the ability to find and filter classes with the CLP property */

@CLP(description = "", hidden = false) abstract class NoOpCommandLineTask extends Pipeline {
  override def build(): Unit = Unit
}

@CLP(description = "", hidden = false) class InClass extends NoOpCommandLineTask

@CLP(description = "", hidden = false) class InClass2 extends Pipeline {
  override def build(): Unit = {}
}

@CLP(description = "", hidden = true) class OutClass extends NoOpCommandLineTask

@CLP(description = "", hidden = true) class Out2Class

class Out3Class

@CLP(description = "", hidden = false) trait OutClass4

/** For testing simple name collisions */
object DirOne {
  @CLP(description = "", hidden = true) class CollisionPipeline extends Pipeline {
    override def build(): Unit = Unit
  }
}
object DirTwo {
  @CLP(description = "", hidden = true) class CollisionPipeline extends Pipeline {
    override def build(): Unit = Unit
  }
}
