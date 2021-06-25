[![Build Status](https://travis-ci.com/fulcrumgenomics/dagr.svg?branch=master)](https://travis-ci.com/fulcrumgenomics/dagr)
[![Coverage Status](https://codecov.io/github/fulcrumgenomics/dagr/coverage.svg?branch=master)](https://codecov.io/github/fulcrumgenomics/dagr?branch=master)
[![Code Review](https://api.codacy.com/project/badge/grade/52e1d786d9784c7192fae2f8e853fa34)](https://www.codacy.com/app/contact_32/dagr)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.fulcrumgenomics/dagr_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.fulcrumgenomics/dagr_2.13)
[![Javadocs](http://javadoc.io/badge/com.fulcrumgenomics/dagr_2.13.svg)](http://javadoc.io/doc/com.fulcrumgenomics/dagr_2.13)
[![License](http://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/fulcrumgenomics/dagr/blob/master/LICENSE)
[![Language](http://img.shields.io/badge/language-scala-brightgreen.svg)](http://www.scala-lang.org/)

# Dagr

A task and pipeline execution system for directed acyclic graphs to support scientific, and more specifically, genomic analysis workflows.
We are currently in alpha development; please see the [Roadmap](#roadmap).
The latest API documentation can be found [here](http://javadoc.io/doc/com.fulcrumgenomics/dagr_2.13).

<!---toc start-->
  * [Goals](#goals)
  * [Building](#building)
  * [Command line](#command-line)
  * [Authors](#authors)
  * [License](#license)
  * [Include Dagr in your project](#include-dagr-in-your-project)
  * [Using DAGR](#using-dagr) - documentation on features and concepts in DAGR

<!---toc end-->

## Goals

There are many toolkits available for creating and executing pipelines of dependent jobs; dagr does not aim to be all things to all people but to make certain types of pipelines easier and more pleasurable to write.  It is specifically focused on:

* Writing pipelines that are concise, legible, and type-safe
* Easy composition of pipelines into bigger pipelines
* Providing safe and coherent ways to dynamically change the graph during execution
* Making the full power and expressiveness of [scala](http://www.scala-lang.org/) available to pipeline authors
* Efficiently executing tasks concurrently within the constraints of a single machine/instance

It is a tool for working data scientists, programmers and bioinformaticians.

## Building

DAGR uses [sbt](https://www.scala-sbt.org/).  Installation instructions are available [here](https://www.scala-sbt.org/download.html).

To build an executable jar run: `sbt assembly`.
Tests may be run with: `sbt test`.

## Command line

DAGR is run with: `java -jar target/scala-2.13/dagr-1.0.0-SNAPSHOT.jar`
Running the above with no options will produce the usage documentation.

## Include dagr in your project

You can include the three sub-projects that make up dagr using:

```
libraryDependencies += "com.fulcrumgenomics" %%  "dagr-core" % "1.0.0"
libraryDependencies += "com.fulcrumgenomics" %%  "dagr-tasks" % "1.0.0"
libraryDependencies += "com.fulcrumgenomics" %%  "dagr-pipelines" % "1.0.0"
```

Or you can depend on the following which will pull in the three dependencies above:

```
libraryDependencies += "com.fulcrumgenomics" %% "dagr" % "1.0.0",
```

## Authors

* [Tim Fennell](https://github.com/tfenne) (maintainer)
* [Nils Homer](https://github.com/nh13) (maintainer)

## License

`dagr` is open source software released under the [MIT License](https://github.com/fulcrumgenomics/dagr/blob/master/LICENSE).


# Using DAGR

The following sections contain an overview of key features and principles behind DAGR that will be useful for anyone working with DAGR pipelines.

## Understanding DAGR's pipeline model

The atom in DAGR's model is called a [Task](https://github.com/fulcrumgenomics/dagr/blob/master/core/src/main/scala/dagr/core/tasksystem/Task.scala).  There are many kinds of tasks, from ones that run a specific command line or execute a small piece of code, up to `Pipeline`s that are tasks which manage the construction of chains of other tasks.  The two main sub-types of Task are:

1. [Pipelines](https://github.com/fulcrumgenomics/dagr/blob/master/core/src/main/scala/dagr/core/tasksystem/Pipeline.scala) which chain together one or more other tasks (including other pipelines)
2. [UnitTasks](https://github.com/fulcrumgenomics/dagr/blob/master/core/src/main/scala/dagr/core/tasksystem/UnitTask.scala) which perform some task when executed.  These can be tasks which run a command line inside a shell, or tasks that run some scala code within the JVM.

Pipelines in DAGR are written in scala, with the aid of an internal DSL for specifying and managing dependencies among other things.

DAGR pipelines are Directed Acyclic GRaphs (hence the name), meaning that a graph is contructed where the tasks are the nodes and the dependencies are the edges between the nodes.  While a programming language might control flow with with conditional constructs (e.g. if statements), these don't fit into a graph model.  In DAGR we allow some conditionality by defering construction of as much of the graph as possible until the last moment possible.

It's helpful to understand when code in Task and Pipeline classes is run:

* Code in the constructors of tasks and pipelines is run immediately just like with any other class.  Therefore code that relies on the existence of input files etc. may not work as expected if placed in the constructor, because input files may not exist yet.
* Code in the `build()` method of Pipelines, and code in the `args()` method of `ProcessTask`  are executed when all dependencies of the pipeline or task have been met.  As a result code run in these methods can generally rely on inputs existing.

## Example Pipeline

The following is an example of a [simple Example pipeline in dagr](pipelines/src/main/scala/dagr/pipelines/ExamplePipeline.scala), minus import and package statements:

```scala
@clp(description="Example FASTQ to BAM pipeline.", group = classOf[Pipelines])
class ExamplePipeline
( @arg(flag="i", doc="Input FASTQ.")        val fastq: PathToFastq,
  @arg(flag="r", doc="Reference FASTA.")    val ref: PathToFasta,
  @arg(flag="t", doc="Target regions.")     val targets: Option[PathToIntervals] = None,
  @arg(flag="o", doc="Output directory.")   val out: DirPath,
  @arg(flag="p", doc="Output file prefix.") val prefix: String
) extends Pipeline(Some(out)) {

  override def build(): Unit = {
    val bam    = out.resolve(prefix + ".bam")
    val tmpBam = out.resolve(prefix + ".tmp.bam")
    val metricsPrefix: Some[DirPath] = Some(out.resolve(prefix))
    Files.createDirectories(out)

    val bwa   = new BwaMem(fastq=fastq, ref=ref)
    val sort  = new SortSam(in=Io.StdIn, out=tmpBam, sortOrder=SortOrder.coordinate)
    val mark  = new MarkDuplicates(in=tmpBam, out=Some(bam), prefix=metricsPrefix)
    val rmtmp = new DeleteBam(tmpBam)

    root ==> (bwa | sort) ==> mark ==> rmtmp
    targets.foreach(path => root ==> new CollectHsMetrics(in=bam, prefix=metricsPrefix, targets=path, ref=ref))
  }
}
```

The `@clp` and `@arg` annotations are required to expose this pipeline for execution via the command line interface. These annotations come from the [sopt project](https://github.com/fulcrumgenomics/sopt) which DAGR uses for command line parsing.  For pipelines that do not need to be run via the command line (for example if they are only used as building blocks in other pipelines) they can be omitted.

## Dependency Language

As can be seen in the example, DAGR uses a number of operators to wire together tasks in a pipeline.  The following are all part of the dependency DSL:

* `task1 ==> task2` creates a dependency that requires that `task1` completes successfully before `task2` is started.  This operator can be chained, e.g. `a ==> b ==> c`
* `root ==> task` adds `task` to the set of root tasks (i.e. those without dependencies) for a pipeline.  `root` is a keyword in the DSL that can be though of as equivalent to `this`.
* `task1 :: task2` creates an object that can be used as a shorthand to mean "task1 and task2".  The result can be used anywhere a Task can be used, including chaining the operator: `a :: b :: c`

It is worth noting that no harm is done by adding a dependency more than once.  E.g. it is perfectly ok to write:

```scala
root ==> (a :: b :: c) ==> (d :: e)
root ==> (a :: c) ==> (e :: f) ==> g
```

### Using `Option`

In the dependency DSL, anywhere you can use a `Task` you can also use an `Option[Task]`.  For example:

```scala
...
val trim: Option[Int] = ...
val trimTask = trim.map(len => new TrimBam(in=bam, out=trimmedBam, length=len))
root ==> trimTask ==> nextTask
...
```

In the above example if `trim` is a `Some(int)` then we end up with a chain of `root => TrimBam ==> nextTask`.  On the other hand if `trim` is None, then the dependency is automatically reduced to `root ==> nextTask`.

### Examples

#### Example 1

```scala
root ==> a ==> (b :: c) ==> d
```

This example sets up the dependencies so that `a` will run immediately when the pipeline starts running, with `b` and `c` depending on `a` and finally with `d` depending on both `b` and `c`.

#### Example 2

```scala
val clipTasks = inputBams.map(b => new MarkIlluminaAdapters(in=b, out=Paths.get("trimmed." + b))
val merge = new MergeSamFiles(in=clipTasks.map(_.in), out=Paths.get("merged.bam"))
root ==> clippedTasks.reduce(_ :: _) ==> merge
```

* The first line creates a `MarkIlluminaAdapters` task for each input BAM.
* The second line creates a `MergeSamFiles` task taking the inputs of all the marking tasks and creating a merged BAM
* The third line uses the `reduce()` collection function along with the `::` operator to combine all the marking tasks into a single dependency group, and then wires them in before the merge task

## Special Tasks & Classes

DAGR has a number of "special" tasks that are useful in their own right and also illustrate how to take advantage of the dynamic nature of DAGR's graph building.

### `Linker`

The [Linker](https://github.com/fulcrumgenomics/dagr/blob/master/core/src/main/scala/dagr/core/tasksystem/Linker.scala) object creates a mechanism to safely copy state from one task to another when the first task finishes running.  This can be useful when one task computes a value that is not written to a file, that is needed by another task.  The following is a toy example that demonstrates usage:

```scala
val input: PathToBam = ...
val counter = new SimpleInJvmTask {
  var count: Option[Int] = None
  def run(): Unit = { count = Some(SamSource(input).iterator.size) }
}
val downsample = new DownsampleSam(in=input, out=Paths.get("downsampled.bam"), target=1e6.toInt)
Linker(counter, downsample, (c, d) => d.inputReads = c.count)
```

The example shows one task that counts the reads in a BAM file and exposes the result in a `var` on the task when it's done running.  The count is then needed by the `downsample` task in order to determine how best to reach the target number of reads.  The `Linker` takes in the two tasks and a function that is to be run when the first task completes successfully, which transfers the count.

### EitherTask

The [EitherTask](https://github.com/fulcrumgenomics/dagr/blob/master/core/src/main/scala/dagr/core/tasksystem/EitherTask.scala) is similar to an `Either` in scala and other functional languages.  It can be thought of as a decision node in the pipeline, which goes left or right depending on some value that isn't yet known.  For example if a different duplicate marking algorithm should be used depending on whether the data has UMIs or not:

```scala
val umiCheck = new SimpleInJvmTask {
  var hasUmis: Boolean = false
  def run(): Unit = { hasUmis = SamSource(input).headOption.exists(_.get("RX").isDefined }
}
val deduper = Either.of(new MarkDuplicates(in=input, out=deduped), new SamBlaster(in=input, out=deduped), umiCheck.hasUmis)
root ==> umiCheck ==> deduper
```

## Shell Piping

DAGR also has a built in DSL and facilities for wiring together tasks that should stream information using shell pipes and redirects.  The piping is even typesafe to prevent, for example, accidentally piping SAM data into a process that expects FASTQ.  To support piping tasks must implement one of:

* [Pipe[A,B]](https://github.com/fulcrumgenomics/dagr/blob/master/core/src/main/scala/dagr/core/tasksystem/Pipe.scala#L51) a trait that marks the task as supporting piping in type `A` and piping out type `B`
* [PipeIn[A]](https://github.com/fulcrumgenomics/dagr/blob/master/core/src/main/scala/dagr/core/tasksystem/Pipe.scala#L98) a trait that states the program can accept piped input of type `A`
* [PipeOut[B]](https://github.com/fulcrumgenomics/dagr/blob/master/core/src/main/scala/dagr/core/tasksystem/Pipe.scala#L101) a trait that states the task can produce piped output of type `B`

For example the `FastqToSam` task is defined as follows:

```scala
class FastqToSam(fastq1: PathToFastq,
                 fastq2: Option[PathToFastq] = None,
                 out: PathToBam,
                 ...)
  extends PicardTask with Pipe[Fastq,SamOrBam]
```

To use piping you might write something like:

```scala
val unmappedBam  = output.resolve("unmapped.bam")
val fqToSam      = new FastqToSam(fastq1=fastq, out=Io.StdOut)
val markAdapters = new MarkIlluminaAdapters(in=Io.StdIn, out=Io.StdOut)
val makeUnmapped = fqToSam | markAdapters > unmappedBam
root ==> makeUnmapped
```

The following operators are supported for tasks that support piping:

* `a | b` pipes stdout from a to b's stdin
* `a > path` redirects stdout from `a` into a file at `path`
* `a >> path` redirects stdout from `a` and appends it to the file at `path`
* `a >! path` redirects stderr from `a` into a file at `path`
* `a >>! path` redirects stderr from `a` and appends it to the file at `path`

Any types can be used when extending/implementing `Pipe`.  A number of types common to bioinformatics are defined in the [tasks package](https://github.com/fulcrumgenomics/dagr/blob/master/tasks/src/main/scala/dagr/tasks/package.scala#L43).

Note that because scala can mix-in traits at runtime, if you find a task you want to use piping with (and the tool supports reading from stdin or writing to stdout) but doesn't currently implement `Pipe`, you can add it on the fly.  For exsample if you want to downsample a BAM only to generate a single set of metrics from it:

```scala
Seq(0.25, 0.5, 0.75).foreach { frac =>
  val ds = new DownsampleSam(in=input, out=Io.StdOut) with PipeOut[SamOrBam]
  val isize = new InsertSizeMetrics(in=Io.StdIn, out=Paths.get(s"isize.$frac")) with PipeIn[SamOrBam]
  root ==> (ds | isize)
}
```

## Resource Management

DAGR schedules tasks for execution when all their dependencies have been met and when there are sufficient resources.  Tasks in DAGR must therefore define the resources they need to execute.  The resources managed by DAGR are cpu cores and memory.

Pipelines themselves do not consume resources, but the concrete tasks ([UnitTasks](https://github.com/fulcrumgenomics/dagr/blob/master/core/src/main/scala/dagr/core/tasksystem/UnitTask.scala)) within pipelines do.  Tasks manage their resources needs through a pair of functions:

```scala
def pickResources(availableResources: ResourceSet): Option[ResourceSet]
def applyResources(resources : ResourceSet): Unit
```

The first function is called by DAGR to inform the task of the available resources, and ask the task a) whether it can run with the available resources and b) what subset of those resources it would like.  The second function is called by DAGR to inform the task of it's allocated resources immediately prior to execution.  While these functions can be implemented for each task, they are usually implemented by mixing in one of the following traits:

* [FixedResources](https://github.com/fulcrumgenomics/dagr/blob/master/core/src/main/scala/dagr/core/tasksystem/Schedulable.scala#L169) allows tasks to define a fixed amount of CPU cores and memory through a number of `requires()` functions
* [VariableResources](https://github.com/fulcrumgenomics/dagr/blob/master/core/src/main/scala/dagr/core/tasksystem/Schedulable.scala#L207) if the task can take advantage of more resources when they are available.

For example a tool which can be multi-threaded but requires a fixed amount of memory per thread might extend `VariableResources` and define the following function to pick it's resources:

```scala
override def pickResources(resources: ResourceSet): Option[ResourceSet] = {
  val memPerThread = Memory("2G")
  val maxThreads = 48
  val cpus = Range.inclusive(start = 48, end = 2, step= -1)
  cpus.flatMap(c => resources.subset(Cores(c), Memory(c * memPerThread.value).headOption
}
```

What this does is attempt to allocate 48 cpu cores, then 47 etc. down to 2, with 2GB per core.  The first combination that fits within the available resources (`ResourceSet.subset()` returns `Option[ResourceSet]`, yielding the requested resources if they fit and `None` if they don't) is then returned.  If no combination fits, `None` is returned and the task isn't scheduled yet.

## Retry

DAGR has the ability to retry tasks that fail.  The default behaviour is to try a task once and if it fails, it stays failed.  This can be changed by mixing in the [Retry](https://github.com/fulcrumgenomics/dagr/blob/master/core/src/main/scala/dagr/core/tasksystem/Retry.scala) trait to a task either when the task class is defined, or when the task is instantiated.  The trait defines one function:

```scala
def retry(resources: SystemResources, taskInfo: TaskExecutionInfo): Boolean = false
```

If, when invoked, the function returns `true` then the task will be retried.  Note that the task may modify it's internal state (arguments, resource requirements, etc.) when `retry()` is invoked to increase the chance of success.

A number of helper traits are available:

* `LinearMemoryRetry` will increase the memory used by a fixed increment each retry up to some maximum memory limit
* `MemoryDoublingRetry` will double the allocated memory on each retry up to some maximum memory limit
* `JvmRanOutOfMemory` is a trait that can be used with either of the above traits when the task is executing a Java program.  It examines the log to determine if the failure was caused by running out of memory, and prevents retry if the failure was not memory-related.
* `MultipleRetry` will retry a task a fixed number of times without altering anything. This can be useful if a task is prone to failure due to e.g. intermittent network connectivity problems.


## Scatter / Gather

DAGR includes a concise framework for performing scatter/gather operations, i.e. breaking an input into multiple partitions based on some criteria, processing each partition in parallel and then merging the results.  A common example of this in variant calling where the genome is broken down into non-overlapping sets of regions which are called independently and then the resulting VCFs are merged to create a final VCF.  DAGR's implementation supports the ability to chain together multiple tasks per partition before gathering.

Building a scatter/gather workflow is relatively simple and has few requirements beyond a normal workflow:

1. A task that implements the `Partitioner` trait
2. A task that can merge the results of the processing each partition

An example pipeline exists in DAGR that uses the GATK to genotype a sample using a chain of `HaplotypeCaller`, `GenotypeGVCFs` and `FilterVcf` on each partition.  The full example can be found in the [ParallelHaplotypeCaller](https://github.com/fulcrumgenomics/dagr/blob/master/pipelines/src/main/scala/dagr/pipelines/ParallelHaplotypeCaller.scala) workflow.

A simplified version follows:

```scala
import dagr.tasks.ScatterGather.Scatter
val scatterer = new SplitIntervalsForCallingRegions(ref=ref, intervals=intervals, output=Some(dir), maxBasesPerScatter=5e6.toInt)
val scatter = Scatter(scatterer)
val hc      = scatter.map(il => new HaplotypeCaller(ref=ref, intervals=Some(il), bam=input, vcf=PathUtil.replaceExtension(il, ".g.vcf.gz"))
val gt      = hc.map(h => GenotypeGvcfs(ref=ref, intervals=h.intervals, gvcf=h.vcf, vcf=replace(h.vcf, ".g.vcf.gz", ".vcf.gz")))
val filter  = gt.map(g => new FilterVcf(in=g.vcf, out=replace(g.vcf, ".vcf.gz", ".filtered.vcf.gz")))
val gather  = filter.gather(fs => new GatherVcfs(in=fs.map(_.out), out=output))

root ==> scatter
gather ==> new DeleteFiles(dir)
```

There's a lot going on in this short code snippet!

1. `SplitIntervalsForCallingRegions` is a task that breaks apart an interval list into many smaller interval lists. It also implements `Partitioner[PathToIntervals]` which allows the scatter/gather framework to use it to initiate a scatter operation.
2. On line 3 we instantiate a `Scatter` object which is used to wire together and manage all the tasks in the scatter/gather pipeline
3. On line 4 we use the `.map()` method on `Scatter` to create a `HaplotypeCaller` task per partition.  The `.map()` method takes as a parameter a function that is invoked once per input partition to create the jobs we need.
4. On lines 5 and 6 we continue the scattered processing by extending the chain with further calls to `.map()`.  In this case the function we provide to `.map()` receives as input the prior task in the chain.
5. On line 7 we use the `.gather()` function to generate a task that gathers the results of all the scatters.  It receives as input the last task from each branch of the scatter, and uses their outputs as the input to `GatherVcfs`.
6. Lastly on lines 9-10 we wire in the dependencies.  The framework has already hooked up all the dependencies between parts of the scatter/gather workflow so all that's left is to wire `scatter` to it's dependencies, and add any tasks that come afterwards

It should be noted that all of the objects generated during a scatter/gather workflow (e.g. `scatter`, `hc`, `gt`, `filter`) can be `.map()'d` and `.gather()'d` more than once.  I.e. you can build a scatter/gather pipeline with multiple branches and multiple endpoints without repeating any work.

