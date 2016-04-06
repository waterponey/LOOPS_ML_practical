package MovieRecommender

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by maryanmorel on 05/04/16.
  */
object Main{

  case class Params(size: String = "ml-100k")

  def main(args: Array[String]) {

    if (args.length != 1) {
      println("""
                | Usage:
                |
                | spark-submit --class Main \
                |  target/scala-2.10/movierecomenderals_2.10-1.0.jar <MovieLens data size>
                |
                |  where <MovieLens data size> can be equal to
                |  ml-100k
                |  ml-1m
                |  ml-10m
              """.stripMargin)
      sys.exit(1)
    }

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    // set up environment
    val jarFile = "movierecommenderals_2.10-1.0.jar"
    val conf = new SparkConf()
      .setAppName("MovieLensALS")
      .setJars(Seq(jarFile))
    val sc = new SparkContext(conf)

    val movieLensHomeDir = "hdfs:///movielens/" + args(0)

    val recommendationEngine = new MovieRecommenderALS(sc, movieLensHomeDir)
    // val recommendationEngine = new Solutions(sc, movieLensHomeDir)
    recommendationEngine.run()

    // Clean up
    sc.stop()
  }

}
