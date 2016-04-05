import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

/**
  * Created by maryanmorel on 05/04/16.
  */
object Main{

  def main(args: Array[String]) {

    if (args.length != 1) {
      println("Usage: sbt/sbt package \"run movieLensHomeDir\"")
      sys.exit(1)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // set up environment
    val jarFile = "target/scala-2.10/movierecommenderals_2.10-1.0.jar"
    val sparkHome = sys.env("SPARK_HOME")
    val master = Source.fromFile("/root/spark-ec2/cluster-url").mkString.trim
    val masterHostname = Source.fromFile("/root/spark-ec2/masters").mkString.trim
    val conf = new SparkConf()
      .setMaster(master)
      .setSparkHome(sparkHome)
      .setAppName("MovieLensALS")
      //    .set("spark.executor.memory", "2g")
      .setJars(Seq(jarFile))
    val sc = new SparkContext(conf)

    val movieLensHomeDir = "hdfs://" + masterHostname + ":9000" + args(0)

    val recommendationEngine = new MovieRecommenderALS(sc, movieLensHomeDir)
    recommendationEngine.run()
  }
}
