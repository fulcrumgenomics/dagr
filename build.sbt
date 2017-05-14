import com.typesafe.sbt.SbtGit.GitCommand
import sbt.Keys._
import sbt._
import sbtassembly.AssemblyKeys.assembly
import sbtassembly.MergeStrategy
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import scoverage.ScoverageKeys._

////////////////////////////////////////////////////////////////////////////////////////////////
// We have the following "settings" in this build.sbt:
// - versioning with sbt-release
// - custom JAR name for the root project
// - settings to publish to Sonatype
// - exclude the root, tasks, and pipelines project from code coverage
// - scaladoc settings
// - custom merge strategy for assembly
////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////
// Use sbt-release to bump the version numbers.
//
// see: http://blog.byjean.eu/2015/07/10/painless-release-with-sbt.html
////////////////////////////////////////////////////////////////////////////////////////////////

// Release settings
releaseVersionBump := sbtrelease.Version.Bump.Next
releasePublishArtifactsAction := PgpKeys.publishSigned.value
releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  ReleaseStep(action = Command.process("publishSigned", _)),
  setNextVersion,
  commitNextVersion,
  ReleaseStep(action = Command.process("sonatypeReleaseAll", _)),
  pushChanges
)

////////////////////////////////////////////////////////////////////////////////////////////////
// For the aggregate (root) jar, override the name.  For the sub-projects,
// see the build.sbt in each project folder.
////////////////////////////////////////////////////////////////////////////////////////////////
assemblyJarName in assembly := "dagr-" + version.value + ".jar"

////////////////////////////////////////////////////////////////////////////////////////////////
// Sonatype settings
////////////////////////////////////////////////////////////////////////////////////////////////
publishMavenStyle := true
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value)
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}
publishArtifact in Test := false
pomIncludeRepository := { _ => false }
// For Travis CI - see http://www.cakesolutions.net/teamblogs/publishing-artefacts-to-oss-sonatype-nexus-using-sbt-and-travis-ci
credentials ++= (for {
  username <- Option(System.getenv().get("SONATYPE_USER"))
  password <- Option(System.getenv().get("SONATYPE_PASS"))
} yield Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", username, password)).toSeq

////////////////////////////////////////////////////////////////////////////////////////////////
// Coverage settings: only count coverage of dagr.core
////////////////////////////////////////////////////////////////////////////////////////////////
coverageExcludedPackages := "<empty>;dagr\\.tasks.*;dagr\\.pipelines.*;dagr\\.cmdline.*"
val htmlReportsDirectory: String = "target/test-reports"

////////////////////////////////////////////////////////////////////////////////////////////////
// scaladoc options
////////////////////////////////////////////////////////////////////////////////////////////////
val docScalacOptions = Seq("-groups", "-implicits")

////////////////////////////////////////////////////////////////////////////////////////////////
// Common settings for all projects
////////////////////////////////////////////////////////////////////////////////////////////////

lazy val commonSettings = Seq(
  organization         := "com.fulcrumgenomics",
  organizationName     := "Fulcrum Genomics LLC",
  organizationHomepage := Some(url("http://www.fulcrumgenomics.com")),
  homepage             := Some(url("http://github.com/fulcrumgenomics/dagr")),
  startYear            := Some(2015),
  scalaVersion         := "2.11.11",
  crossScalaVersions   := Seq("2.11.11", "2.12.2"),
  scalacOptions        += "-target:jvm-1.8",
  scalacOptions in (Compile, doc) ++= docScalacOptions,
  scalacOptions in (Test, doc) ++= docScalacOptions,
  autoAPIMappings := true,
  testOptions in Test  += Tests.Argument(TestFrameworks.ScalaTest, "-h", Option(System.getenv("TEST_HTML_REPORTS")).getOrElse(htmlReportsDirectory)),
  testOptions in Test  += Tests.Argument("-l", "LongRunningTest"), // ignores long running tests
  // uncomment for full stack traces
  //testOptions in Test  += Tests.Argument("-oDF"),
  fork in Test         := true,
  resolvers            += Resolver.jcenterRepo,
  resolvers            += Resolver.sonatypeRepo("public"),
  resolvers            += Resolver.mavenLocal,
  shellPrompt          := { state => "%s| %s> ".format(GitCommand.prompt.apply(state), version.value) },
  coverageExcludedPackages := "<empty>;dagr\\.tasks.*;dagr\\.pipelines.*",
  updateOptions        := updateOptions.value.withCachedResolution(true),
  javaOptions in Test += "-Ddagr.color-status=false",
  // Needed to avoid "sbt.ForkMain failed with exit code 137"
  //in Travis with `sudo: false`.
  // See https://github.com/sbt/sbt/issues/653
  // and https://github.com/travis-ci/travis-ci/issues/3775
  javaOptions in Test += "-Xmx1G",
  libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test->*" excludeAll ExclusionRule(organization="org.junit", name="junit")
) ++ Defaults.coreDefaultSettings

////////////////////////////////////////////////////////////////////////////////////////////////
// core project
////////////////////////////////////////////////////////////////////////////////////////////////
lazy val core = Project(id="dagr-core", base=file("core"))
  .settings(commonSettings: _*)
  .settings(description := "Core methods and classes to execute tasks in dagr.")
  .settings(
    libraryDependencies ++= Seq(
      "com.fulcrumgenomics" %%  "commons"           %  "0.2.0",
      "com.fulcrumgenomics" %%  "sopt"              %  "0.2.0",
      "com.github.dblock"   %   "oshi-core"         %  "3.3",
      "org.scala-lang"      %   "scala-reflect"     %  scalaVersion.value,
      "org.scala-lang"      %   "scala-compiler"    %  scalaVersion.value,
      "org.reflections"     %   "reflections"       %  "0.9.10",
      "com.typesafe"        %   "config"            %  "1.3.1",
      "javax.servlet"       %   "javax.servlet-api" %  "3.1.0"
    )
  )
  .disablePlugins(sbtassembly.AssemblyPlugin)

////////////////////////////////////////////////////////////////////////////////////////////////
// tasks project
////////////////////////////////////////////////////////////////////////////////////////////////
lazy val htsjdkAndPicardExcludes = Seq(
  ExclusionRule(organization="org.apache.ant"),
  ExclusionRule(organization="gov.nih.nlm.ncbi"),
  ExclusionRule(organization="org.testng"),
  ExclusionRule(organization="com.google.cloud.genomics")
)

lazy val tasks = Project(id="dagr-tasks", base=file("tasks"))
  .settings(description := "A set of example dagr tasks.")
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Seq(
      "com.github.samtools"       % "htsjdk"     % "2.8.1" excludeAll(htsjdkAndPicardExcludes: _*),
      "com.github.broadinstitute" % "picard"     % "2.8.1" excludeAll(htsjdkAndPicardExcludes: _*)
    )
  )
  .disablePlugins(sbtassembly.AssemblyPlugin)
  .dependsOn(core)

////////////////////////////////////////////////////////////////////////////////////////////////
// pipelines project
////////////////////////////////////////////////////////////////////////////////////////////////
lazy val pipelines = Project(id="dagr-pipelines", base=file("pipelines"))
  .settings(description := "A set of example dagr pipelines.")
  .settings(commonSettings: _*)
  .disablePlugins(sbtassembly.AssemblyPlugin)
  .dependsOn(tasks, core)

////////////////////////////////////////////////////////////////////////////////////////////////
// root (dagr) project
////////////////////////////////////////////////////////////////////////////////////////////////
lazy val assemblySettings = Seq(
  test in assembly     := {},
  logLevel in assembly := Level.Info
)
lazy val root = Project(id="dagr", base=file("."))
  .settings(commonSettings: _*)
  .settings(unidocSettings: _*)
  .settings(assemblySettings: _*)
  .settings(description := "A tool to execute tasks in directed acyclic graphs.")
  .aggregate(core, tasks, pipelines)
  .dependsOn(core, tasks, pipelines)

////////////////////////////////////////////////////////////////////////////////////////////////
// Merge strategy for assembly
////////////////////////////////////////////////////////////////////////////////////////////////
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
      case ps@(x :: xt) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xt =>
        MergeStrategy.discard
      case "spring.tooling" :: xt =>
        MergeStrategy.discard
      case "com.google.guava" :: xt =>
        MergeStrategy.discard
      case "services" :: xt =>
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
