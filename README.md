[![Build Status](https://travis-ci.org/fulcrumgenomics/dagr.svg?branch=master)](https://travis-ci.org/fulcrumgenomics/dagr)
[![Coverage Status](https://codecov.io/github/fulcrumgenomics/dagr/coverage.svg?branch=master)](https://codecov.io/github/fulcrumgenomics/dagr?branch=master)
[![Code Review](https://api.codacy.com/project/badge/grade/52e1d786d9784c7192fae2f8e853fa34)](https://www.codacy.com/app/contact_32/dagr)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.fulcrumgenomics/dagr_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.fulcrumgenomics/dagr_2.11)
[![Dependency Status](https://www.versioneye.com/user/projects/56b2d2d593b95a003c714340/badge.svg)](https://www.versioneye.com/user/projects/56b2d2d593b95a003c714340#dialog_dependency_badge)
[![Javadocs](http://javadoc.io/badge/com.fulcrumgenomics/dagr_2.12.svg)](http://javadoc.io/doc/com.fulcrumgenomics/dagr_2.12)
[![License](http://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/fulcrumgenomics/dagr/blob/master/LICENSE)
[![Language](http://img.shields.io/badge/language-scala-brightgreen.svg)](http://www.scala-lang.org/)

Dagr
====

A task and pipeline execution system for directed acyclic graphs to support scientific, and more specifically, genomic analysis workflows.
We are currently in alpha development; please see the [Roadmap](#roadmap).
The latest API documentation can be found [here](http://javadoc.io/doc/com.fulcrumgenomics/dagr_2.12).

<!---toc start-->
  * [Goals](#goals)
  * [Building](#building)
  * [Command line](#command-line)
  * [Include Dagr in your project](#include-dagr-in-your-project)
  * [Roadmap](#roadmap)
  * [Overview](#overview)
  * [List of features](#list-of-features)
  * [Authors](#authors)
  * [License](#license)

<!---toc end-->

## Goals

There are many toolkits available for creating and executing pipelines of dependent jobs; dagr does not aim to be all things to all people but to make certain types of pipelines easier and more pleasurable to write.  It is specifically focused on:

* Writing pipelines that are concise, legible, and type-safe
* Easy composition of pipelines into bigger pipelines
* Providing safe and coherent ways to dynamically change the graph during execution
* Making the full power and expressiveness of [scala](http://www.scala-lang.org/) available to pipeline authors
* Efficiently executing tasks concurrently within the constraints of a single machine/instance 

It is a tool for working data scientists, programmers and bioinformaticians.

## Example

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

The `@clp` and `@arg` annotations are required to expose this pipeline for execution via the command line interface. For pipelines that do not need to be run via the command line (for example if they are only used as building blocks in other pipelines) they can be omitted.

## Building 

Use ```sbt assembly``` to build an executable jar in ```target/scala-2.11/```.  
Tests may be run with ```sbt test```.

## Command line

`java -jar target/scala-2.11/dagr-0.1.0-SNAPSHOT.jar` to see the full list of options.

## Include dagr in your project

You can include the three sub-projects that make up dagr using:

```
libraryDependencies += "com.fulcrumgenomics" %%  "dagr-core" % "0.1.0"
libraryDependencies += "com.fulcrumgenomics" %%  "dagr-tasks" % "0.1.0"
libraryDependencies += "com.fulcrumgenomics" %%  "dagr-pipelines" % "0.1.0"
```

Or you can depend on the following which will pull in the three dependencies above:

```
libraryDependencies += "com.fulcrumgenomics" %% "dagr" % "0.1.0",
```

## Roadmap

We are currently working on the first release of `dagr` and therefore rapidly evolving features are subject-to-change.

## Overview

`dagr` contains three projects:

1. `dagr-core` for specifying, scheduling, and executing tasks with dependencies.
2. `dagr-tasks` for common genomic analysis tasks, such as those in [Picard tools](https://github.com/broadinstitute/picard), [JeanLuc](https://github.com/fulcrumgenomics/JeanLuc), [Bwa](https://github.com/lh3/bwa), and elsewhere.
3. `dagr-pipelines` for common genomic pipelines, such as mapping, variant calling, and quality control.

`dagr` endeavors to combine the full features of the Scala programming language with a simplifying DSL for fast and easy authoring of complicated tasks and pipelines.

`dagr` pipelines execute on a single-host or machine.  For resource-intense pipelines, we recommend provisioning large compute instances.

Please see the [example `dagr` configuration](src/main/resources/example.conf) for customizing dagr for your environment.

## List of features

In no particular order ...

* Manages complex dependencies among tasks and pipelines.
* Operators to pipe input and output between tasks and files without writing to disk.
* Resource-aware scheduling across tasks and pipelines to maximize parallelism.
* A simple gnu-style option parser and sub-command system, making pipelines into first-class command line programs.
* Supports pre-compiled pipelines and pipelines from scala script files that are compiled on the fly.
* Tasks are not fully realized until all dependencies are met, allowing for conditional logic.
  * See [EitherTask](core/src/main/scala/dagr/core/tasksystem/EitherTask.scala) for a good example.
* Tasks can execute processes or be pure scala methods run in the JVM.
* Mechanisms for passing state between tasks without coupling the tasks or having to rely on manual co-ordination (ex. storing an intermediate result on disk).
  * See [Linker](core/src/main/scala/dagr/core/tasksystem/Linker.scala)
* Contains a small set of predefined genomic analysis tasks and pipelines.
* Configuration (using [HOCON](https://github.com/typesafehub/config/blob/master/HOCON.md) and [TypeSafe config](https://github.com/typesafehub/config) to fully specify the dagr environment.

## Authors

* [Tim Fennell](https://github.com/tfenne) (maintainer)
* [Nils Homer](https://github.com/nh13) (maintainer)

## License

`dagr` is open source software released under the [MIT License](https://github.com/fulcrumgenomics/dagr/blob/master/LICENSE).
