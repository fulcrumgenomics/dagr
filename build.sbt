import sbt._
import sbt.Keys._
import sbtassembly.AssemblyKeys.assembly
import sbtassembly.MergeStrategy
import sbtrelease.ReleasePlugin._
import com.typesafe.sbt.SbtGit.GitCommand

// for the aggregate (root) jar, override the name.  For the sub-projects, see the build.sbt in each project folder.
assemblyJarName in assembly := "dagr-" + version.value + ".jar"

releaseSettings

val htmlReportsDirectory: String = "target/test-reports"

// Common settings for all projects
lazy val commonSettings = Seq(
  organization         := "com.fulcrumgenomics",
  organizationName     := "Fulcrum Genomics LLC",
  homepage             := Some(url("http://github.com/fulcrumgenomics/dagr")),
  startYear            := Some(2015),
  scalaVersion         := "2.11.7",
  scalacOptions        += "-target:jvm-1.8",
  testOptions in Test  += Tests.Argument(TestFrameworks.ScalaTest, "-h", Option(System.getenv("TEST_HTML_REPORTS")).getOrElse(htmlReportsDirectory)),
  testOptions in Test  += Tests.Argument("-l", "LongRunningTest"), // ignores long running tests
  // uncomment for full stack traces
  //testOptions in Test  += Tests.Argument("-oD"),
  fork in Test         := true,
  test in assembly     := {},
  logLevel in assembly := Level.Info,
  resolvers            += Resolver.jcenterRepo,
  shellPrompt          := { state => "%s| %s> ".format(GitCommand.prompt.apply(state), version.value) }
) ++ Defaults.coreDefaultSettings

lazy val sopt = Project(id="dagr-sopt", base=file("sopt"))
  .settings(commonSettings: _*)
  .settings(description := "Scala command line option parser.")
  .settings(
    libraryDependencies ++= Seq(
      //---------- Test libraries -------------------//
      "org.scalatest"      %%  "scalatest"       %  "2.2.4" % "test->*" excludeAll ExclusionRule(organization="org.junit", name="junit")
    )
  )

lazy val core = Project(id="dagr-core", base=file("core"))
  .settings(commonSettings: _*)
  .settings(description := "Core methods and classes to execute tasks in dagr.")
  .settings(
    libraryDependencies ++= Seq(
      "com.github.dblock"  %   "oshi-core"       %  "2.0",
      "org.scala-lang"     %   "scala-reflect"   %  scalaVersion.value,
      "org.scala-lang"     %   "scala-compiler"  %  scalaVersion.value,
      "org.reflections"    %   "reflections"     %  "0.9.10",
      "com.typesafe"       %   "config"          %  "1.3.0",
      "javax.servlet"      %   "servlet-api"     %  "2.5",
      //---------- Test libraries -------------------//
      "org.scalatest"      %%  "scalatest"       %  "2.2.4" % "test->*" excludeAll ExclusionRule(organization="org.junit", name="junit")
    )
  )
  .dependsOn(sopt)

lazy val tasks = Project(id="dagr-tasks", base=file("tasks"))
  .settings(description := "A set of example dagr tasks.")
  .settings(commonSettings: _*)
  .dependsOn(core)

lazy val pipelines = Project(id="dagr-pipelines", base=file("pipelines"))
  .settings(description := "A set of example dagr pipelines.")
  .settings(commonSettings: _*)
  .dependsOn(tasks, core)

lazy val root = Project(id="dagr", base=file("."))
  .settings(commonSettings: _*)
  .settings(description := "A tool to execute tasks in directed acyclic graphs.")
  .aggregate(sopt, core, tasks, pipelines)
  .dependsOn(sopt, core, tasks, pipelines)


val customMergeStrategy: String => MergeStrategy = {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps@_*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs@_*) =>
    xs map {
      _.toLowerCase
    } match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps@(x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "spring.tooling" :: xs =>
        MergeStrategy.discard
      case "com.google.guava" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.deduplicate
    }
  case "asm-license.txt" | "overview.html" =>
    MergeStrategy.discard
  case "logback.xml" =>
    MergeStrategy.first
  case _ => MergeStrategy.deduplicate
}

assemblyMergeStrategy in assembly := customMergeStrategy
