import com.typesafe.sbt.SbtGit.GitCommand
import sbt._
import sbt.Keys._
import sbtassembly.AssemblyKeys.assembly

object DagrBuild extends Build {

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

  lazy val root = Project(id="dagr", base=file("."))
    .settings(commonSettings: _*)
    .settings(description := "A tool to execute tasks in directed acyclic graphs.")
    .aggregate(sopt, core, tasks, pipelines)
    .dependsOn(sopt, core, tasks, pipelines)

  lazy val sopt = Project(id="dagr-sopt", base=file("dagr/sopt"))
    .settings(commonSettings: _*)
    .settings(description := "Scala command line option parser.")
    .settings(
      libraryDependencies ++= Seq(
        //---------- Test libraries -------------------//
        "org.scalatest"      %%  "scalatest"       %  "2.2.4" % "test->*" excludeAll ExclusionRule(organization="org.junit", name="junit")
      )
    )

  lazy val core = Project(id="dagr-core", base=file("dagr/core"))
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

  lazy val tasks = Project(id="dagr-tasks", base=file("dagr/tasks"))
    .settings(description := "A set of example dagr tasks.")
    .settings(commonSettings: _*)
    .dependsOn(core)

  lazy val pipelines = Project(id="dagr-pipelines", base=file("dagr/pipelines"))
    .settings(description := "A set of example dagr pipelines.")
    .settings(commonSettings: _*)
    .dependsOn(tasks, core)
}
