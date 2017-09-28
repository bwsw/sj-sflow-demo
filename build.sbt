name := "sj-sflow-demo"

version := "1.0"

scalaVersion := "2.12.1"

addCommandAlias("rebuild", ";clean; compile; package")

val commonSettings = Seq(
  version := "1.0",
  scalaVersion := "2.12.1",
  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-feature"
  ),

  //resolvers += "Sonatype OSS" at "https://oss.sonatype.org/service/local/staging/deploy/maven2",
  resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  libraryDependencies ++= Seq(
    "com.bwsw" %% "sj-engine-core" % "1.0-SNAPSHOT" % "provided",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",
    "com.bwsw" %% "sj-engine-simulators" % "1.0-SNAPSHOT" % "test",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"),

  assemblyMergeStrategy in assembly := {
    case PathList("org", "apache", "commons", "logging", xs@_*) => MergeStrategy.first
    case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.concat
    case "log4j.properties" => MergeStrategy.concat
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
    libraryDependencies += "com.hazelcast" % "hazelcast" % "3.7.3"
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
