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
			resolvers += "Apache HBase" at "https://repository.apache.org/content/repositories/releases",
			resolvers += "Thrift" at "http://people.apache.org/~rawson/repo/",
 			libraryDependencies ++= Seq(
//				"com.typesafe.akka" % "akka-actor" % "2.0.2",
//				"com.netflix.astyanax" % "astyanax" % "1.0.6",
//				"com.google.guava" % "guava" % "13.0",
//				"org.codehaus.jackson" % "jackson-core-asl" % "1.9.9",
//				"org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.9",
				"org.apache.hadoop" % "hadoop-core" % "0.20.2",
				"org.apache.hbase" % "hbase" % "0.90.5"
			),

			mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
				case n if n.startsWith("javax/") => MergeStrategy.first
				case n if n.startsWith("jline") => MergeStrategy.first
				case n if n.startsWith("com/eaio/") => MergeStrategy.first
				case n if n.startsWith("org/apache") => MergeStrategy.first
				case x => old(x)
			}}
    )
  )	
}
