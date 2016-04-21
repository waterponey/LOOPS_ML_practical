name := "MovieRecommenderALS"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"

libraryDependencies ++= Seq(
    "org.eclipse.jetty" % "jetty-webapp" % "8.1.7.v20120910" % "compile",
    "org.eclipse.jetty.orbit" % "javax.servlet" % "3.0.0.v201112011016" % "compile" artifacts Artifact("javax.servlet", "jar", "jar")
)

libraryDependencies += "junit" % "junit" % "4.12"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.5.2"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10" % "1.5.2"
