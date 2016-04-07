# LOOPS practical session: Machine learning with Spark

#### Loops presentation: [REX Machine learning](http://maryanmorel.github.io/LOOPS_ML_practical)

A large part of this material is taken from [AMPCAMP 5](http://ampcamp.berkeley.edu/5/)

One of the most common uses of big data is to predict what users want.
This allows Google to show relevant ads, Amazon to recommend relevant products,
and Netflix to recommend movies.

This lab will show you how to use Spark to train a very simple recommender system using
[MLlib](https://spark.apache.org/mllib/) and the [MovieLens dataset](http://grouplens.org/datasets/movielens/).

You are going to use a technique called [collaborative filtering](https://en.wikipedia.org/?title=Collaborative_filtering).

The idea is to make predictions about the interest of a user by collecting preferences
from many other users. The underlying assumption is that if a person A has the same opinion as a person B on an issue,
A is more likely to have B's opinion on a different issue x than to have the opinion on x
of a person chosen randomly. [More informations here](http://recommender-systems.org/collaborative-filtering/).

The image below (from [Wikipedia](https://en.wikipedia.org/?title=Collaborative_filtering)) shows an example
of predicting of the user's rating using collaborative filtering. At first, people rate different items
(like videos, images, games). After that, the system is making predictions about a user's rating for an
item, which the user has not rated yet. These predictions are built upon the existing ratings of other
users, who have similar ratings with the active user. For instance, in the image below the system has made
a prediction, that the active user will not like the video.

![collaborative filtering](https://courses.edx.org/c4x/BerkeleyX/CS100.1x/asset/Collaborative_filtering.gif)

For movie recommendations, we start with a matrix whose entries are movie ratings by users
(shown in red in the diagram below).  Such matrices are called "user-item" matrices. In our case,
items are movies. Each column represents a user (shown in green) and each row represents a particular movie (shown in blue).

Since not all users have rated all movies, we do not know all of the entries in this matrix,
which is precisely why we need collaborative filtering.  For each user, we have ratings for
only a subset of the movies.  The idea of collaborative filtering is to approximate the ratings
matrix by factorizing it as the product of two matrices: one that describes properties of each user
(shown in green), and one that describes properties of each movie (shown in blue).

![factorization](http://spark-mooc.github.io/web-assets/images/matrix_factorization.png)

We want to estimate these two matrices such that the error for the users/movie pairs where we
know the correct ratings is minimized.  The [Alternating Least Squares](https://en.wikiversity.org/wiki/Least-Squares_Method)
algorithm does this by first randomly filling the users matrix with values and then optimizing the value
of the movies such that the error is minimized.  Then, it holds the movies matrix constant and optimizes
the value of the user's matrix.

## Setup

First, if you don't have them already, please download and install:
- [Java JDK 1.8](http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
- [Scala 2.10.4](http://www.scala-lang.org/download/2.10.4.html)
- [sbt](http://www.scala-sbt.org/)

Then download and decompress Spark 1.5.2 [here](http://d3kbcqa49mib13.cloudfront.net/spark-1.5.2-bin-hadoop2.6.tgz).
To run the tests, you will need to set the environment variable `SPARK_HOME`

    $ export SPARK_HOME=/path/to/spark-1.5.2-bin-hadoop2.6

If you want to use an IDE, you can go with [IntelliJ IDEA](https://www.jetbrains.com/idea/download/) or [Eclipse](http://scala-ide.org/).
In this case, don't forget to install the Scala plugin and to set the environment variable `SPARK_HOME`
in your test configuration.

You can also work with your favorite text editor.

Open the file `src/main/scala/MovieRecommenderALS.scala`

In this file, you will find nine questions. For each of them, you
will have to replace `???` with appropriate code. You can get some help with
the documentation of the [Spark Scala API](https://spark.apache.org/docs/1.5.2/api/scala/index.html#org.apache.spark.package)
and the documentation of [MLLib guide](http://spark.apache.org/docs/latest/mllib-collaborative-filtering.html)

To test your answers, you can run the tests that are in `src/test/scala/`. To do so, use you IDE, or in the
project directory, run:

    $ sbt test

This command will compile your code and test it. The last question (9) is not
tested, but pretty simple (i.e. you just have to read MLlib doc to find it out...).

To run the actual program, you will have to package it with your IDE or

    $ sbt package

This will produce the `.jar` file located in `./target/scala-2.10/movierecommenderals_2.10-1.0.jar`
You will need to send this file to the servers provided for this tutorial:

    $ scp target/scala-2.10/movierecommenderals_2.10-1.0.jar user@host:~/

Then, to run it, connect to the server with SSH

    $ ssh user@host
    ## some authentication here
    [user@hadoop01 ~]$ spark-submit --conf spark.executor.instances=4 --executor-cores 4 --executor-memory 6G \
                        --class MovieRecommender.Main movierecommenderals_2.10-1.0.jar ml-100k

You can change the MovieLens dataset you use by replacing `ml-100k` by `ml-1m` or `ml-10m` for the one million and ten
million ratings files.

Enjoy!
