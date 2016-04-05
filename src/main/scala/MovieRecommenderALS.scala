import java.util.Random

import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation.{ALS, Rating, MatrixFactorizationModel}


class MovieRecommenderALS(sc: SparkContext, movieLensHomeDir: String) {

  /** Read the ratings file and parse its lines to get a PairRDD[Long, Rating[Int, Int, Double]]
    * containing (timestamp % 10, Rating(userId, movieId, rating)) */
  def readRatingsFile(path: String): RDD[(Long, Rating)] = {
  }

  /** Read the movies file and parses its lines to get a PairRDD[Int, String]
    * containing (movieId, movieName) */
  def readMoviesFile(path: String): RDD[(Int, String)] = {
  }

  def countRatings(ratings: RDD[(Long, Rating)]): Long = {
  }

  def countUsers(ratings: RDD[(Long, Rating)]): Long = {
  }

  def countMovies(ratings: RDD[(Long, Rating)]): Long = {
  }

  /** Return the ID of the 50 most rated movies */
  def getMostPopularMoviesID(ratings: RDD[(Long, Rating)]): Seq[Int] = {
  }

  /** Using ratings data:
    * Split ratings into train (60%), validation (20%), and test (20%) sets based on the last digit of the timestamp.
    * Use the rating for training if last digit < 6
    * for validation if last digit >= 6 and < 8
    * for testing otherwise
    * Add myRatings to train.
    * The resulting RDDs must be cached to avoid being recomputed at each loop of the cross-validation.*/
  def splitData(ratings: RDD[(Long, Rating)], myRatingsRDD: RDD[Rating]):
                  (RDD[Rating], RDD[Rating], RDD[Rating]) = {
    val numPartitions = 20
    val training = ???
    val validation = ???
    val test = ???
    (training, validation, test)
  }

  /** Compute RMSE (Root Mean Squared Error). */
  def computeRmse(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    ???
  }

  /** Compute the mean rating of the union of the training and validation sets.
    * This will serve as a naive predictor (always predicting the mean rating) to compute a baseline performance.
    */
  def computeMeanRating(training: RDD[Rating], validation: RDD[Rating]): Double = {
    ???
  }

  def computeBaselineRMSE(meanRating: Double, test: RDD[Rating], numTest: Long): Double = {
    ???
  }

  def getRecommendations(bestModel: MatrixFactorizationModel, candidates: RDD[Int]) = {
    ???
  }

  def run() {

    // load ratings and movie titles

    val ratings = readRatingsFile(movieLensHomeDir + "/ratings.dat")
    val movies = readMoviesFile(movieLensHomeDir + "/movies.dat").collect.toMap

    // your code here
    val numRatings = countRatings(ratings)
    val numUsers = countUsers(ratings)
    val numMovies = countMovies(ratings)

    println("Ratings: " + numRatings + ", users: " + numUsers + ", movies: " + numMovies)

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

    println("Training: " + numTraining + ", validation: " + numValidation + ", test: " + numTest)

    // Cross-validation: train models and evaluate them on the validation set
    // Use a function to perform cross-validation
    val ranks = List(8, 12)
    val lambdas = List(0.1, 10.0)
    val numIters = List(10, 20)
    var bestModel: Option[MatrixFactorizationModel] = None
    var bestValidationRmse = Double.MaxValue
    var bestRank = 0
    var bestLambda = -1.0
    var bestNumIter = -1
    for (rank <- ranks; lambda <- lambdas; numIter <- numIters) {
      val model = ALS.train(training, rank, numIter, lambda)
      val validationRmse = computeRmse(model, validation, numValidation)
      println("RMSE (validation) = " + validationRmse + " for the model trained with rank = " 
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

    println("The best model was trained with rank = " + bestRank + " and lambda = " + bestLambda
      + ", and numIter = " + bestNumIter + ", and its RMSE on the test set is " + testRmse + ".")

    // create a naive baseline and compare it with the best model

    val meanRating = computeMeanRating(training, validation)
    val baselineRmse = computeBaselineRMSE(meanRating, test, numTest)
    val improvement = (baselineRmse - testRmse) / baselineRmse * 100
    println("The best model improves the baseline by " + "%1.2f".format(improvement) + "%.")

    // make personalized recommendations
    val myRatedMovieIds = myRatings.map(_.product).toSet
    val candidates = sc.parallelize(movies.keys.filter(!myRatedMovieIds.contains(_)).toSeq)
    val recommendations = getRecommendations(bestModel.get, candidates)

    var i = 1
    println("Movies recommended for you:")
    recommendations.foreach { r =>
      println("%2d".format(i) + ": " + movies(r.product))
      i += 1
    }

    // clean up
    sc.stop()
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
              rating = Some(Rating(0, x._1, r))
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
