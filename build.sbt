name := "MovieRecommenderALS"

version := "1.0"

scalaVersion := "2.10.4"

test in assembly := {}
mainClass in assembly := Some("Main")

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"
libraryDependencies += "junit" % "junit" % "4.12"
libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.2"
