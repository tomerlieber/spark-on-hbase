name := "spark-on-hbase"

organization := "x"

version := "1.0"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.1"
val hBaseVersion = "1.0.3"
val hadoopVersion = "2.6.0"

// TODO: check what is the target of the 'provided' symbol in the end of the library dependency lvy.
libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % sparkVersion % "provided",
  "org.apache.hbase" % "hbase-common" % hBaseVersion % "provided",
  "org.apache.hbase" % "hbase-client" % hBaseVersion % "provided",
  "org.apache.hbase" % "hbase-server" % hBaseVersion % "provided",
  "org.scalatest" % "scalatest_2.10" % "2.2.4" % "test",
  "org.apache.hbase" % "hbase-testing-util" % "1.3.0" % "test")