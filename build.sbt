name := "spark-sampling"

version := "0.1"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.0.0-incubating-SNAPSHOT"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies += "org.apache.commons" % "commons-math" % "2.2"

net.virtualvoid.sbt.graph.Plugin.graphSettings
