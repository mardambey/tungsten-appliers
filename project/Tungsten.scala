import sbt._
import sbt.Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object TungstenBuild extends Build {
	
  lazy val tungsten = Project(
    id = "tungsten",
    base = file("."),
    settings = Project.defaultSettings  ++ assemblySettings ++ Seq(
      name := "Utilities for Tungsten Replicator.",
      organization := "com.mate1",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.9.2",
      resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases",
      libraryDependencies += "com.typesafe.akka" % "akka-actor" % "2.0.2",
			libraryDependencies += "com.netflix.astyanax" % "astyanax" % "1.0.6",
			libraryDependencies += "com.google.guava" % "guava" % "13.0",
			libraryDependencies += "org.codehaus.jackson" % "jackson-core-asl" % "1.9.9",
			libraryDependencies += "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.9",
			     
			mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
				case n if n.startsWith("javax/servlet/") => MergeStrategy.first
				case n if n.startsWith("jline") => MergeStrategy.first
				case n if n.startsWith("com/eaio/") => MergeStrategy.first
				case x => old(x)
			}}
    )
  )	
}
