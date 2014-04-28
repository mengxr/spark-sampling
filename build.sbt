import AssemblyKeys._

name := "spark-sampling"

scalaVersion := "2.10.3"

version := "0.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "0.9.0-incubating" % "provided"

libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1" % "test"

libraryDependencies += "org.apache.commons" % "commons-math3" % "3.2"

net.virtualvoid.sbt.graph.Plugin.graphSettings

assemblySettings

assemblyOption in assembly ~= { _.copy(includeScala = false) }

test in assembly := {}
