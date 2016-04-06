package MovieRecommender

import java.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}

/**
  * Fill in the ??? with the appropriate code.
  * This tutorial has been designed for Scala 2.10.4 and Spark 1.5.2
  * you can download this version of Spark at:
  * http://d3kbcqa49mib13.cloudfront.net/spark-1.5.2-bin-hadoop2.6.tgz
  *
  * Spark documentation can be found here:
  * https://spark.apache.org/docs/1.5.2/index.html
  * Doc of the Scala API:
  * https://spark.apache.org/docs/1.5.2/api/scala/index.html#org.apache.spark.package
  *
  * You can test your code using
  *     sbt test
  * in your terminal or by using any IDE supporting Scala & SBT.
  * The "test suite" should run in 5-20 sec depending on your configuration.
  * The tests require to set the SPARK_HOME environment variable.
  * Please set it in your environment and/or IDE, e.g.
  * $ export SPARK_ENV=/opt/spark/spark-1.5.2-bin-hadoop2.6
  *
  */

class Solutions(sc: SparkContext, movieLensHomeDir: String) {

  /** Question 1a:
    * Read the ratings file and parse its lines to get a PairRDD[Long, Rating[Int, Int, Double]]
    * containing (timestamp % 10, Rating(userId, movieId, rating)) */
  def readRatingsFile(path: String): RDD[(Long, Rating)] = {
    sc.textFile(path).map { line =>
      val fields = line.split("::")
      (fields(3).toLong % 10, Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
    }
  }

  /** Question 1b
    * Read the movies file and parses its lines to get a PairRDD[Int, String]
    * containing (movieId, movieName) */
  def readMoviesFile(path: String): RDD[(Int, String)] = {
    sc.textFile(path).map { line =>
      val fields = line.split("::")
      (fields(0).toInt, fields(1))
    }
  }

  /** Question 2a
    * Count the number of ratings from the ratings file */
  def countRatings(ratings: RDD[(Long, Rating)]): Long = {
    ratings.count
  }

  /** Question 2b
    * Count the number of users from the ratings file */
  def countUsers(ratings: RDD[(Long, Rating)]): Long = {
    ratings.map(_._2.user).distinct.count
  }

  /** Question 2c
    * Count the number of movies from the ratings file */
  def countMovies(ratings: RDD[(Long, Rating)]): Long = {
    ratings.map(_._2.product).distinct.count
  }

  /** Question 3
    * Return the ID of the 50 most rated movies */
  def getMostPopularMoviesID(ratings: RDD[(Long, Rating)]): Seq[Int] = {
    ratings.map(_._2.product) // extract movie ids
      .countByValue      // count ratings per movie
      .toSeq             // convert map to Seq
      .sortBy(- _._2)    // sort by rating count
      .take(50)          // take 50 most rated
      .map(_._1)         // get their ids
  }

  /** Question 4
    * Using ratings data:
    * Split ratings into train (60%), validation (20%), and test (20%) sets.
    * Use the RDD.randomSplit(Array(0.8, 0.2)) method, with seed 42
    * The resulting RDDs must be cached to avoid being recomputed at each loop of the cross-validation. */
  def splitData(ratings: RDD[(Long, Rating)], myRatingsRDD: RDD[Rating]):
                  (RDD[Rating], RDD[Rating], RDD[Rating]) = {
    val numPartitions = 20
    val splits = ratings.randomSplit(Array(0.6, 0.2, 0.2), 42)
    val training = splits(0)
      .values
      .union(myRatingsRDD)
      .repartition(numPartitions)
      .persist
    val validation = splits(1)
      .values
      .repartition(numPartitions)
      .persist
    val test = splits(2).values.persist
    (training, validation, test)
  }

  /** Question 5
    * Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict(data.map(x => (x.user, x.product)))
    val predictionsAndRatings = predictions.map(x => ((x.user, x.product), x.rating))
      .join(data.map(x => ((x.user, x.product), x.rating)))
      .values
    math.sqrt(predictionsAndRatings.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)
  }

  /** Question 6
    * Compute the mean rating of the union of the training and validation sets.
    * This will serve as a naive predictor (always predicting the mean rating) to compute a baseline performance. */
  def computeMeanRating(training: RDD[Rating], validation: RDD[Rating]): Double = {
    training.union(validation).map(_.rating).mean
  }

  /** Question 7
    * Compute a baseline RMSE using the meanRating as prediction of the user rating. */
  def computeBaselineRMSE(meanRating: Double, test: RDD[Rating], numTest: Long): Double = {
    math.sqrt(test.map(x => (meanRating - x.rating) * (meanRating - x.rating))
    .reduce(_ + _) / numTest)
  }

  /** Question 8
    * Recommend the 50 movies whith the higher predicted rating among the candidate movies */
  def getRecommendations(bestModel: MatrixFactorizationModel, candidates: RDD[Int]): Array[Rating] = {
    bestModel.predict(candidates.map((0, _)))
      .collect
      .sortBy(- _.rating)
      .take(50)
  }

  def run() {

    // load ratings and movie titles
    println("\n Reading data from " + movieLensHomeDir)

    val ratings = readRatingsFile(movieLensHomeDir + "/ratings.dat")
    val movies = readMoviesFile(movieLensHomeDir + "/movies.dat").collect.toMap

    // your code here
    val numRatings = countRatings(ratings)
    val numUsers = countUsers(ratings)
    val numMovies = countMovies(ratings)

    println("\n Ratings: " + numRatings + ", users: " + numUsers + ", movies: " + numMovies)

    // sample a subset of most rated movies for rating elicitation
    val mostRatedMovieIds = getMostPopularMoviesID(ratings)

    // Ask the user to rate some movies from the most popular ones
    // Subsample the most popular movies (random)
    val random = new Random(0)
    val selectedMovies = mostRatedMovieIds.filter(x => random.nextDouble() < 0.2)
      .map(x => (x, movies(x)))

    // elicitate ratings
    val myRatings = elicitateRatings(selectedMovies)
    val myRatingsRDD =  sc.parallelize(myRatings, 1) // Create an RDD from the ratings

    // split ratings into train (60%), validation (20%), and test (20%) based on the 
    // last digit of the timestamp, add myRatings to train, and cache them
    val (training, validation, test) = splitData(ratings, myRatingsRDD)

    val numTraining = training.count
    val numValidation = validation.count
    val numTest = test.count

    println("\n Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

    // Cross-validation: train models and evaluate them on the validation set
    // Use a function to perform cross-validation
    val ranks = List(8, 12)
    val lambdas = List(.1, .2)
    val numIters = List(10, 20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)
      println("\n RMSE (validation) = " + validationRmse + " for the model trained with rank = "
        + rank + ", lambda = " + lambda + ", and numIter = " + numIter + ".")
      if (validationRmse < bestValidationRmse) {
        bestModel = Some(model)
        bestValidationRmse = validationRmse
        bestRank = rank
        bestLambda = lambda
        bestNumIter = numIter
      }
    }

    // evaluate the best model on the test set
    val testRmse = computeRmse(bestModel.get, test, numTest)

    println("\n The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
      + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

    // create a naive baseline and compare it with the best model
    val meanRating = computeMeanRating(training, validation)
    val baselineRmse = computeBaselineRMSE(meanRating, test, numTest)
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println("\n The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")

    // make personalized recommendations
    val myRatedMovieIds = myRatings.map(_.product).toSet
    val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
    val recommendations = getRecommendations(bestModel.get, candidates)

    var i = 1
    println("\n Movies recommended for you:")
    recommendations.foreach { r =>
      println("%2d".format(i) + ": " + movies(r.product))
      i += 1
    }
  }
  
  /** Elicitate ratings from command-line. */
  def elicitateRatings(movies: Seq[(Int, String)]) = {
    val prompt = "Please rate the following movie (1-5 (best), or 0 if not seen):"
    println(prompt)
    val ratings = movies.flatMap { x =>
      var rating: Option[Rating] = None
      var valid = false
      while (!valid) {
        print(x._2 + ": ")
        try {
          val r = Console.readInt
          if (r < 0 || r > 5) {
            println(prompt)
          } else {
            valid = true
            if (r > 0) {
              /*
               * MovieLens ratings are on a scale of 1-5:
               * 5: Must see
               * 4: Will enjoy
               * 3: It's okay
               * 2: Fairly bad
               * 1: Awful
               * So we should not recommend a movie if the predicted rating is less than 3.
               * To map ratings to confidence scores, we use
               * 5 -> 2.5, 4 -> 1.5, 3 -> 0.5, 2 -> -0.5, 1 -> -1.5. This mappings means unobserved
               * entries are generally between It's okay and Fairly bad.
               * The semantics of 0 in this expanded world of non-positive weights
               * are "the same as never having interacted at all".
               */
              rating = Some(Rating(0, x._1, r.toDouble - 2.5))
            }
          }
        } catch {
          case e: Exception => println(prompt)
        }
      }
      rating match {
        case Some(r) => Iterator(r)
        case None => Iterator.empty
      }
    }
    if(ratings.isEmpty) {
      sys.error("No rating provided!")
    } else {
      ratings
    }
  }

  /** Define ??? to raise Not Implemented exception */
  def ??? : Nothing = throw new UnsupportedOperationException("Not Implemented")

}
