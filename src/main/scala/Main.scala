import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scopt.OptionParser

/**
  * Created by maryanmorel on 05/04/16.
  */
object Main{

  case class Params(size: String = "ml-100k")

  def main(args: Array[String]) {
    val defaultParams = Params()

    val parser = new OptionParser[Params]("MovieLensALS") {
      head("MovieRecommenderALS: a spark mllib tutorial for ALS on MovieLens data.")
      opt[String]("size")
        .required()
        .text("Size of the MovieLens dataset to use")
        .action((x, c) => c.copy(size = x))
      note(
        """
          |For example, the following command runs this app on a the MovieLens 100k dataset:
          |
          | spark-submit --class Main \
          |  target/scala-2.10/movierecomenderals_2.10-1.0.jar \
          |  ml-100k
        """.stripMargin)
    }

    parser.parse(args, defaultParams).map { params =>
      Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
      Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

      // set up environment
      val jarFile = "target/scala-2.10/movierecommenderals_2.10-1.0.jar"
      val conf = new SparkConf()
        .setAppName("MovieLensALS")
        .setJars(Seq(jarFile))
      val sc = new SparkContext(conf)

      val movieLensHomeDir = "hdfs://movielens/" + params.size

      val recommendationEngine = new MovieRecommenderALS(sc, movieLensHomeDir)
      recommendationEngine.run()
      // Clean up
      sc.stop()
      // All clear
      true
    } getOrElse {
      System.exit(1)
    }
  }

}
