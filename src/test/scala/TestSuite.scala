/**
  * Created by maryanmorel on 05/04/16.
  */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.scalautils._
import TripleEquals._
import Tolerance._


class TestSuite extends FunSuite {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

  // set up environment
  val jarFile = "target/scala-2.10/movierecommenderals_2.10-1.0.jar"
  val sparkHome = sys.env("SPARK_HOME")
  val master = "local[*]"
  val conf = new SparkConf()
    .setMaster(master)
    .setSparkHome(sparkHome)
    .setAppName("MovieLensALS")
    .setJars(Seq(jarFile))
  val sc = new SparkContext(conf)
  sc.setLogLevel("WARN")

  val movieLensPath = getClass.getClassLoader.getResource("movielensdata").getPath
  val recommender = new MovieRecommenderALS(sc, movieLensPath)
  val expectedRecommender = new Solutions(sc, movieLensPath)
  val ratingsPath = getClass.getClassLoader.getResource("movielensdata/ratings.dat.gz").getPath
  val moviesPath = getClass.getClassLoader.getResource("movielensdata/movies.dat").getPath

  val expectedRatings = expectedRecommender.readRatingsFile(ratingsPath)
  val expectedMovies = expectedRecommender.readMoviesFile(moviesPath).collect.toMap

  test("Question1a: read ratings") {
    val ratings = recommender.readRatingsFile(ratingsPath)
    val numRatings = ratings.count
    val top3 = ratings.map(x => x._1.toString + x._2.toString).takeOrdered(3)
    assert(numRatings == 487650)
    val expectedTop = Array("0Rating(1,1193,5.0)", "0Rating(1,1566,4.0)", "0Rating(1,1907,4.0)")
    assert(top3 === expectedTop)
  }

  test("Question1b: read movies") {
    val movies = recommender.readMoviesFile(moviesPath)
    val numMovies = movies.count
    val top3 = movies.map(x => x._1.toString + x._2.toString).takeOrdered(3)
    assert(numMovies == 3883)
    val expectedTop = Array("1000Curdled (1996)", "1001Associate, The (L'Associe)(1982)", "1002Ed's Next Move (1996)")
    assert(top3 === expectedTop)
  }

  test("Question2a: count ratings") {
    val numRatings = recommender.countRatings(expectedRatings)
    assert(numRatings == 487650)
  }

  test("Question2b: count users") {
    val numUsers = recommender.countUsers(expectedRatings)
    assert(numUsers == 2999)
  }

  test("Question2b: count movies") {
    val numMovies = recommender.countMovies(expectedRatings)
    assert(numMovies == 3615)
  }

  test("Question3: get most popular movies ID") {
    val numMovies = recommender.getMostPopularMoviesID(expectedRatings)
    val expected = Array(2858, 260, 1196, 480, 1210, 589, 2028, 1580, 110, 2571, 593,
    608, 1270, 1198, 2396, 527, 1617, 1265, 2762, 2997, 318, 1197, 1097, 2628, 2716,
    858, 356, 296, 3578, 1, 1240, 2791, 457, 2916, 1259, 1214, 2987, 3481, 3114, 3793,
    34, 648, 2291, 541, 1127, 1200, 50, 2355, 3175, 919)
    assert(numMovies === expected)
  }

  test("Question4: split data") {
    val myRatings = expectedRatings.sample(false, .1).map(_._2)
    val (training, validation, test) = recommender.splitData(expectedRatings, myRatings)
    val (expectedTraining, expectedValidation, expectedTest) = expectedRecommender.splitData(expectedRatings, myRatings)
    assert(training.map(_.toString).takeOrdered(3) === expectedTraining.map(_.toString).takeOrdered(3))
    assert(validation.map(_.toString).takeOrdered(3) === expectedValidation.map(_.toString).takeOrdered(3))
    assert(test.map(_.toString).takeOrdered(3) === expectedTest.map(_.toString).takeOrdered(3))
  }

  test("Question5: RMSE on model") {
    val training = expectedRatings.map(_._2)
    val numTraining = training.count
    val model = ALS.train(training, 8, 10, 0.1)
    val rmse = recommender.computeRmse(model, training, numTraining)
    val expectedRmse = expectedRecommender.computeRmse(model, training, numTraining)
    assert(rmse == expectedRmse)
  }

  test("Question6: Compute mean rating and baseline") {
    val myRatings = expectedRatings.sample(false, .1).map(_._2)
    val (training, validation, test) = expectedRecommender.splitData(expectedRatings, myRatings)
    val mean = recommender.computeMeanRating(training, validation)
    val expectedMean = expectedRecommender.computeMeanRating(training, validation)
    assert(mean === expectedMean +- 1e-5)
  }

  test("Question7: compute baseline") {
    val myRatings = expectedRatings.sample(false, .1).map(_._2)
    val (training, validation, test) = expectedRecommender.splitData(expectedRatings, myRatings)
    val expectedMean = expectedRecommender.computeMeanRating(training, validation)
    val numTest = test.count
    val baseline = recommender.computeBaselineRMSE(expectedMean, test, numTest)
    val expectedBaseline = expectedRecommender.computeBaselineRMSE(expectedMean, test, numTest)
    assert(baseline == expectedBaseline)
  }

  test("Question8: get recommendations") {
    val myRatedMovieIds = expectedRatings.sample(false, .1)
                                         .map(_._2)
                                         .collect()
                                         .map(_.product)
                                         .toSet
    val training = expectedRatings.map(_._2)
    val model = ALS.train(training, 8, 10, 0.1)
    val candidates = sc.parallelize(expectedMovies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
    val recommendations = recommender.getRecommendations(model, candidates)
    val expectedRecommendations = expectedRecommender.getRecommendations(model, candidates)
    assert(recommendations === expectedRecommendations)
  }

}
