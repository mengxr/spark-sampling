name := "spark-sampling"

scalaVersion := "2.10.3"

version := "0.1"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.0.0-incubating-SNAPSHOT"

libraryDependencies += "org.scalatest" %% "scalatest" % "1.9.1" % "test"

libraryDependencies += "org.apache.commons" % "commons-math" % "2.2"

net.virtualvoid.sbt.graph.Plugin.graphSettings
