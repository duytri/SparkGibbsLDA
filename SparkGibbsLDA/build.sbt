import sbt._
import Keys._
import sbtassembly.AssemblyPlugin.autoImport._

name := "SparkGibbsLDA"
version := "0.1-SNAPSHOT"
organization := "uit.master.thesis"
scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.1"
)

unmanagedBase <<= baseDirectory { base => base / "libs" }
unmanagedResourceDirectories in Compile += baseDirectory.value / "src" / "main"
excludeFilter in unmanagedResourceDirectories := "*.scala"

scalacOptions += "-deprecation"
assemblyJarName in assembly := name.value+"-"+version.value+".jar"
mainClass in assembly := Some("main.scala.SparkGibbsLDA")
test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "application.conf"                            => MergeStrategy.concat
  case "unwanted.txt"                                => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) =>
      (xs map {_.toLowerCase}) match {
        case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
          MergeStrategy.discard
        case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
          MergeStrategy.discard
        case "plexus" :: xs =>
          MergeStrategy.discard
        case "services" :: xs =>
          MergeStrategy.filterDistinctLines
        case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
          MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.last
      }
  case x                                	     => MergeStrategy.last
}
