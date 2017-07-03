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

package dagr.pipelines

import dagr.core.cmdline.Pipelines
import dagr.core.tasksystem._
import com.fulcrumgenomics.sopt.{arg, clp}
import dagr.api.models.{Cores, Memory, ResourceSet}

private trait GreedyResourcePicking extends UnitTask {
  override def pickResources(availableResources: ResourceSet): Option[ResourceSet] = {
    val mem = Memory("1g")
    (8 to 1 by -1).map(c => ResourceSet(Cores(c), mem)).find(rs => availableResources.subset(rs).isDefined)
  }
}

private class SleepProcessTask(seconds: Int = 1) extends ProcessTask with GreedyResourcePicking {
  override def args: Seq[Any] = "sleep" :: s"$seconds" :: Nil
}

private class SleepInJvmTask(seconds: Int = 1) extends SimpleInJvmTask with GreedyResourcePicking {
  def run(): Unit = {
    logger.info(s"Sleeping for $seconds")
    Thread.sleep(seconds * 1000)
    logger.info(s"I'm awake!")
  }
}

/**
  * Very simple example pipeline that creates random tasks and dependencies
  */
@clp(description="A bunch of sleep tasks.", group = classOf[Pipelines])
class SleepyPipeline
( @arg(flag='j', doc="Use JVM tasks") val jvmTask: Boolean = false,
  @arg(flag='n', doc="The number of tasks to create") val numTasks: Int = 100,
  @arg(flag='p', doc="The probability of creating a dependency") val dependencyProbability: Double = 0.1,
  @arg(flag='s', doc="The seed for the random number generator") val seed: Option[Long] = None,
  @arg(flag='S', doc="The time for each task to sleep in seconds") val sleepSeconds: Int = 1,
  @arg(flag='f', doc="The failure rate of tasks") val failureRate: Double = 0.0
) extends Pipeline {
  val randomNumberGenerator = seed match {
    case Some(s) => new scala.util.Random(s)
    case None    => scala.util.Random
  }

  private def toATask: (Int) => Task = (s) => {
    if (randomNumberGenerator.nextFloat() < failureRate) {
      ShellCommand("exit", "1")
    }
    else {
      new SleepProcessTask(s)
    }
  }
  private def toBTask: (Int) => Task = (s) => {
    if (randomNumberGenerator.nextFloat() < failureRate) {
      SimpleInJvmTask.apply(name = "Name", f = { if (true) throw new IllegalArgumentException("failed") else Unit })
    }
    else {
      new SleepInJvmTask(s)
    }
  }
  private val toTask   = if (jvmTask) toBTask else toATask
  private val taskType = if (jvmTask) "JVM" else "Shell"


  override def build(): Unit = {
    // create the tasks
    val tasks: Seq[Task] = for (i <- 0 to numTasks) yield toTask(this.sleepSeconds) withName s"task-$i"

    // make them depend on previous tasks
    var rootTasks = Seq.range(start=0, numTasks).toSet

    for (i <- 0 until numTasks) {
      for (j <- 0 until i) {
        if (randomNumberGenerator.nextFloat < dependencyProbability) {
          logger.info(s"Task $i will depend on task $j")
          tasks(j) ==> tasks(i)
          rootTasks = rootTasks - i
        }
      }
    }

    rootTasks.foreach { i =>
      root ==> tasks(i)
    }
  }
}

