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

package dagr.core.tasksystem

import dagr.core.UnitSpec

/**
 * Test that are specific to the Pipeline class
 */
class PipelineTest extends UnitSpec {
  def named(s: String) = new NoOpInJvmTask(s)

  "Pipeline" should "not touch task names if no prefix or suffix is given" in {
    class TestPipeline extends Pipeline {
      override def build(): Unit = root ==> named("hello") ==> named("world")
    }
    val ts = new TestPipeline().getTasks.map(_.name).toList.sorted
    ts shouldBe List("hello", "world")
  }

  it should "add prefixes only to task names when there is no suffix" in {
    class TestPipeline extends Pipeline(prefix=Some("foo.")) {
      override def build(): Unit = root ==> named("hello") ==> named("world")
    }
    val ts = new TestPipeline().getTasks.map(_.name).toList.sorted
    ts shouldBe List("foo.hello", "foo.world")
  }

  it should "add suffixes only to task names when there is no prefix" in {
    class TestPipeline extends Pipeline(suffix=Some(".foo")) {
      override def build(): Unit = root ==> named("hello") ==> named("world")
    }
    val ts = new TestPipeline().getTasks.map(_.name).toList.sorted
    ts shouldBe List("hello.foo", "world.foo")
  }

  it should "add prefixes and suffixes to task names when both are supplied" in {
    class TestPipeline extends Pipeline(prefix=Some("before."), suffix=Some(".after")) {
      override def build(): Unit = root ==> named("hello") ==> named("world")
    }
    val ts = new TestPipeline().getTasks.map(_.name).toList.sorted
    ts shouldBe List("before.hello.after", "before.world.after")
  }

  it should "stack prefixes and suffixes for nested pipelines" in {
    class InnerPipeline extends Pipeline(prefix=Some("innerbefore."), suffix=Some(".innerafter")) {
      override def build(): Unit = root ==> named("hello") ==> named("world")
    }
    class OuterPipeline extends Pipeline(prefix=Some("outerbefore."), suffix=Some(".outerafter")) {
      override def build(): Unit = root ==> new InnerPipeline
    }

    val ts = new OuterPipeline().make().head.make().map(_.name).toList.sorted
    ts shouldBe List("outerbefore.innerbefore.hello.outerafter.innerafter",
                     "outerbefore.innerbefore.world.outerafter.innerafter")
  }
}
