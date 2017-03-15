name := "sj-sflow-demo"

version := "1.0"

scalaVersion := "2.12.1"

addCommandAlias("rebuild", ";clean; compile; package")

val commonSettings = Seq(
  scalaVersion := "2.12.1",
  scalacOptions ++= Seq(
    "-unchecked",
    "-deprecation",
    "-feature"
  ),

  resolvers += "Sonatype OSS" at "https://oss.sonatype.org/service/local/staging/deploy/maven2",
  resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
  libraryDependencies ++= Seq(
    "com.bwsw" %% "sj-engine-core" % "1.0-SNAPSHOT",
    "org.apache.avro" % "avro" % "1.8.1"
  ),

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

lazy val sflowDemoInput = Project(id = "sflow-input",
  base = file("./sflow-input"))
  .settings(commonSettings: _*)

lazy val sflowDemoProcess = Project(id = "sflow-process",
  base = file("./sflow-process"))
  .settings(commonSettings: _*)

lazy val sflowDemoOutput = Project(id = "sflow-output",
  base = file("./sflow-output"))
  .settings(commonSettings: _*)
