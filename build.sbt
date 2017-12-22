name := "sj-sflow-demo"

val demoVersion = "1.0-SNAPSHOT"

addCommandAlias("rebuild", ";clean; compile; package")

val commonSettings = Seq(
  version := demoVersion,
  scalaVersion := Dependencies.Versions.scala,
  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-feature"
  ),

  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  homepage := Some(url("https://github.com/bwsw/sj-sflow-demo.git")),
  pomIncludeRepository := { _ => false },
  parallelExecution in Test := false,
  organization := "com.bwsw",
  publishMavenStyle := true,
  pomIncludeRepository := { _ => false },
  isSnapshot := true,

  pomExtra :=
    <scm>
      <url>git@github.com/bwsw/sj-sflow-demo.git</url>
      <connection>scm:git@github.com/bwsw/sj-sflow-demo.git</connection>
    </scm>
      <developers>
        <developer>
          <id>bitworks</id>
          <name>Bitworks Software, Ltd.</name>
          <url>http://bitworks.software/</url>
        </developer>
      </developers>,


  publishTo in ThisBuild := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases" at nexus + "service/local/staging/deploy/maven2")
  },
  publishArtifact in Test := false,


  resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  resolvers += "Elasticsearch Releases" at "https://artifacts.elastic.co/maven",
  
  libraryDependencies ++= Dependencies.sjSflowCommonDependencies.value,

  assemblyMergeStrategy in assembly := {
    case PathList("org", "apache", "commons", "logging", xs@_*) => MergeStrategy.first
    case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.concat
    case "log4j.properties" => MergeStrategy.concat
    case PathList("io", "netty", xs@_*) => MergeStrategy.first
    case x =>
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
  },

  assemblyJarName in assembly := s"${name.value}-${version.value}.jar",

  fork in run := true,
  fork in Test := true,
  parallelExecution in Test := false
)

lazy val root = (project in file(".")) aggregate(
  sflowDemoProcess,
  sflowDemoSrcIpOutput,
  sflowDemoSrcDstOutput,
  sflowDemoFallbackOutput
)

lazy val sflowDemoCommon = Project(id = "sflow-common",
  base = file("./sflow-common"))
  .settings(commonSettings: _*)

lazy val sflowDemoProcess = Project(id = "sflow-process",
  base = file("./sflow-process"))
  .dependsOn(sflowDemoCommon)
  .settings(commonSettings: _*)
  .settings(
    libraryDependencies ++= Dependencies.sjSflowProcessDependencies.value
  )

lazy val sflowDemoSrcIpOutput = Project(id = "sflow-src-ip-output",
  base = file("./sflow-output/src-ip"))
  .dependsOn(sflowDemoCommon)
  .settings(commonSettings: _*)

lazy val sflowDemoSrcDstOutput = Project(id = "sflow-src-dst-output",
  base = file("./sflow-output/src-dst"))
  .dependsOn(sflowDemoCommon)
  .settings(commonSettings: _*)

lazy val sflowDemoFallbackOutput = Project(id = "sflow-fallback-output",
  base = file("./sflow-fallback-output"))
  .settings(commonSettings: _*)
