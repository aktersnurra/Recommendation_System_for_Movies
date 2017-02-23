package org.BachelorThesis

/**
  * Created by Gustaf on 2016-03-17.
  */

import java.io.{File, PrintWriter}

import breeze.numerics.{pow, sqrt}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer
import scala.io.Source

object KmeansClustering {
  //For Windows usage.
  //System.setProperty("hadoop.home.dir", "C:\\Users\\Aktersnurra\\Dropbox\\KTH\\Åk3\\KEX\\Kodgruvan\\winutil")

  //Path to the database file on both mac and PC.
  val pathToDatabasePC = "C:\\Users\\Aktersnurra\\Dropbox\\KTH\\Åk3\\KEX\\Kodgruvan\\resources\\Databas\\omdb0216\\omdbMovies.txt"
  val pathToDatabaseMac = "/Users/Gustaf/Dropbox KTH/Dropbox/KTH/Åk3/KEX/Kodgruvan/resources/Databas/omdb0216/omdbMovies.txt"

  // Initilize spark for the recommendation system.
  val conf = new SparkConf()
    .setAppName("RecommendationSystem")
    .setMaster("local")
  val sc = new SparkContext(conf)

  //Used when saving maps with unique names.
  var i: Int = 0

  //Numbers to give unique movie features a number. Starts at 2.0 because the logarithm of the numbers will be used.
  var numA: Double = 1.0
  var numD: Double = 1.0
  var numG: Double = 1.0
  var numL: Double = 1.0
  var maxActor, maxDirector, maxYear, maxGenre, maxRating, maxVotes, maxLanguage: Double = 0.0
  var minActor, minDirector, minGenre, minLanguage: Double = 0.0
  var minYear = 1900.0
  var minVotes = 10000.0
  var minRating = 6.0

  //Actor, Actor, Actor, Director, Year, Genre, Genre, Rating, Votes, Language
  val upperBoundary: List[Double] = List(9.0, 9.0, 9.0, 18.0, 3.0, 10.5, 10.5, 2.0, 5.5, 80.0)


  //Maps used for checking if a movie feature has already been giving a unique number.
  var castMap, directorMap, countryMap, yearMap, genreMap, languageMap, writerMap: Map[String, Double] = Map("" -> 0.0)

  //Arrays and ListBuffer that will contain Movie objects.
  var movieArray, movieArray2, movieArray3 = Array[Movie]()
  var movieArray4: ListBuffer[Movie] = null

  //KMeansModel initialized for access in functions.
  var clusters: KMeansModel = null

  //Initialized here for access in functions
  var trainingData: RDD[Movie] = null

  //Initialized here for access in functions
  var parsedData: RDD[Vector] = null
  var featureArray: Array[String] = null

  var checkForDuplicate: ListBuffer[(String, String, String)] = null

  /**
    * Sorts the database on the rating and number of votes the movie has on IMDb. Only returns true if number of votes are > 1000 and
    * the movie has a rating of > 5.0.
    *
    * @param line Each line in in the text file (which is a movie).
    * @return true if the movie has a rating of > 5.0 and number of votes > 1000.
    */
  def sortWithIMDBStats(line: String): Boolean = {
    val tempList = line.split("\t")
    val imdbRating = tempList(12)
    val imdbVotes = tempList(13)
    val Title = tempList(2)
    val Released = tempList(7)
    val Runtime = tempList(5)
    val movie = (Title, Released, Runtime)

    if (imdbRating != "" && imdbVotes != "") {
      if (imdbRating.toDouble > 4.0 && imdbVotes.toInt > 1000) {
        if (checkForDuplicate.size != 0 && checkForDuplicate.contains(movie)) {
          return false
        } else {
          checkForDuplicate += movie
          return true
        }
      }

      else {
        return false
      }
    }

    return false

  }


  /**
    * Creates Movie object of each film, returns an array containing all Movie objects.
    *
    * @param line Each line in in the text file (which is a movie).
    */
  def getMovie(line: String): Unit = {
    val tempList = line.split("\t")
    val Title = tempList(2).split(",")(0)
    val movie = new Movie(line)
    vectorizer(movie)
    movieArray :+= movie
  }


  /**
    * Gets a Movie object as input, returns the Movie object with an updated vector representation of the movie.
    * The movie vector is modelled as [3 first actors in the cast, Director, Year,] with a unique number (<tt>Double<\tt>)
    * for each unique attribute.
    *
    * @param movie a Movie object.
    */
  def vectorizer(movie: Movie): Unit = {
    //Create the array containing the movie profile.
    val tempArrayDY = Array(getDirectorVal(movie), getYearVal(movie))
    val tempArrayRVL = Array(getRatingVal(movie), getVotesVal(movie), getLanguageVal(movie))
    val movieArray = getCastArray(movie) ++ tempArrayDY ++ getGenreArray(movie) ++ tempArrayRVL

    //Creating the movie vector.
    val movieVector = Vectors.dense(movieArray.mkString(" ").split(' ').map(_.toDouble))

    movie.setMovieVector(movieVector)
  }


  /**
    * Converts actors to numbers. Saves each actor that is given a unique number to the castMap. This is done to prevent
    * that an actor that already has a unique number will be given another one.
    *
    * @param movie a Movie object
    * @return an array containing a unique logarithmic value for each actor.
    */
  def getCastArray(movie: Movie): Array[Double] = {
    //Get the 3 first actors in the cast.
    val Cast = movie.getCast()
    //Initilize an array that will contain the actors numbers.
    val castArray = new Array[Double](3)
    var actorVal = 0.0
    //Temp index for the array.
    var i = 0

    //Will go through all actors in the cast and check if the actor already has a unique number or not.
    for (actor <- Cast) {
      if (castMap.contains(actor)) {
        actorVal = castMap(actor)
        castArray(i) = actorVal
        i += 1
      }

      else {
        actorVal = numA
        castMap += (actor -> actorVal)
        castArray(i) = actorVal
        numA += 1.0
        i += 1
      }
      if (maxActor < actorVal) maxActor = actorVal
    }
    castArray
  }


  /**
    * Get a unique logarithmic number for the director.
    *
    * @param movie a Movie object.
    * @return a unique logarithmic value for the director.
    */
  def getDirectorVal(movie: Movie): Double = {
    val Director = movie.getDirector()
    var directorValue = 0.0

    if (directorMap.contains(Director)) {
      directorValue = directorMap(Director)
    }

    else {
      directorValue = numD
      directorMap += (Director -> directorValue)
      numD += 1.0
    }
    if (maxDirector < directorValue) maxDirector = directorValue
    directorValue
  }

  /**
    * Get a unique logarithmic number for a language.
    *
    * @param movie a Movie object.
    * @return a unique logarithmic value for the language.
    */
  def getLanguageVal(movie: Movie): Double = {
    val language = movie.getLanguage()
    var languageValue = 0.0

    if (languageMap.contains(language)) {
      languageValue = languageMap(language)
    }

    else {
      languageValue = numL
      languageMap += (language -> languageValue)
      numL += 1.0
    }
    if (maxLanguage < languageValue) maxLanguage = languageValue

    languageValue
  }


  /**
    * Get a unique logarithmic number for the year.
    *
    * @param movie a Movie object.
    * @return a unique logarithmic value for the year.
    */
  def getYearVal(movie: Movie): Double = {
    val Year = movie.getYear()
    var yearValue = 0.0

    if (yearMap.contains(Year)) {
      yearValue = yearMap(Year)
    }

    else {
      yearValue = Year.toDouble
      yearMap += (Year -> yearValue)
    }

    if (maxYear < yearValue) maxYear = yearValue
    if (minYear > yearValue) minYear = yearValue
    yearValue
  }


  /**
    * Converts the two first genres of the movie to unique logarithmic values.
    *
    * @param movie a Movie object.
    * @return an array of the logarithmic numbers of the two genres.
    */
  def getGenreArray(movie: Movie): Array[Double] = {
    val Genres = movie.getGenre()
    val genreArray = new Array[Double](2)
    var i = 0
    var genreVal = 0.0

    for (genre <- Genres) {
      if (genreMap.contains(genre)) {
        genreVal = genreMap(genre)
        genreArray(i) = genreVal
        i += 1
      }
      else {
        genreVal = numG
        genreMap += (genre -> genreVal)
        genreArray(i) = genreVal
        numG += 1.0
        i += 1
      }
      if (maxGenre < genreVal) maxGenre = genreVal
    }
    genreArray
  }


  def getRatingVal(movie: Movie): Double = {
    val imdbRating = movie.getimdbRating().toDouble
    if (maxRating < imdbRating) maxRating = imdbRating
    if (minRating > imdbRating) minRating = imdbRating
    imdbRating
  }

  def getVotesVal(movie: Movie): Double = {
    val imdbVotes = movie.getimdbVotes().toDouble
    if (maxVotes < imdbVotes) maxVotes = imdbVotes
    if (minVotes > imdbVotes) minVotes = imdbVotes
    imdbVotes
  }

  def minmaxNorm(movie: Movie): Unit = {
    var i = 0
    val movieVector = movie.getMovieVector.toArray
    val normArray = new Array[Double](10)
    val maxValues = List(maxActor, maxActor, maxActor, maxDirector, maxYear, maxGenre, maxGenre, maxRating, maxVotes, maxLanguage)
    val minValues = List(minActor, minActor, minActor, minDirector, minYear, minGenre, minGenre, minRating, minVotes, minLanguage)
    for (element <- movieVector) {
      val xPrime = ((element - minValues(i)) / (maxValues(i) - minValues(i))) * upperBoundary(i)
      normArray(i) = xPrime
      i += 1
    }
    movie.setMovieVector(Vectors.dense(normArray.mkString(" ").split(' ').map(_.toDouble)))
  }


  def minmaxNorm(movieVector: Array[Double]): Array[Double] = {
    var i = 0
    val normArray = new Array[Double](10)
    val maxValues = List(maxActor, maxActor, maxActor, maxDirector, maxYear, maxGenre, maxGenre, maxRating, maxVotes, maxLanguage)
    val minValues = List(minActor, minActor, minActor, minDirector, minYear, minGenre, minGenre, minRating, minVotes, minLanguage)
    for (element <- movieVector) {
      val xPrime = ((element - minValues(i)) / (maxValues(i) - minValues(i))) * upperBoundary(i)
      normArray(i) = xPrime
      i += 1
    }
    normArray
  }


  /**
    * Preforms the calculation of getting the unit vector.
    *
    * n = v / |v|
    *
    * @param movieVector an array containing the values of each movies features.
    * @return the normalized array with the length of 1.
    */
  def getUnitVector(movieVector: Array[Double]): Array[Double] = {
    val lengthOfVector = getVectorLength(movieVector)
    val unitVector = movieVector map {
      _ * (1 / lengthOfVector)
    }
    unitVector
  }


  /**
    * Evaluate clustering by computing Within Set Sum of Squared Errors. Used in order to determine a good
    * number of clusters. This is done by plotting the WSSSE against the number of clusters and then look at where
    * the elbow is in the graph.
    *
    * @param parsedData a data set containing the movie vectors.
    */
  def getElbow(parsedData: RDD[Vector]): Unit = {
    val numClustersList = Array(50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180, 190, 200, 210, 220, 230, 240, 250, 260, 270, 280, 290, 300)
    var elbowArray = Array[Double]()
    for (numClusters <- numClustersList) {
      val numIterations = 40
      val clusters = KMeans.train(parsedData, numClusters, numIterations, 1, "k-means||")

      // Evaluate clustering by computing Within Set Sum of Squared Errors
      val WSSSE = clusters.computeCost(parsedData)
      //println("Within Set Sum of Squared Errors = " + WSSSE)

      elbowArray :+= WSSSE.toDouble
    }
    println(elbowArray.mkString(" "))
  }


  /**
    * Predicts the cluster each moive belongs to and assigns the cluster ID to each Movie object.
    * Appends all movies with the cluster ID to an array.
    *
    * @param movie a Movie object.
    */
  def predictCluster(movie: Movie): Unit = {
    val movieVector = movie.getMovieVector
    val clusterID = clusters.predict(movieVector)
    movie.setClusterID(clusterID)
    movieArray2 :+= movie
  }


  /**
    * Method to save the maps. Use to check if everything seems correct.
    *
    * @param map a map of the movies different attributes.
    */
  def saveMap(map: Map[String, Double]): Unit = {
    val writer = new PrintWriter(new File("map" + i + ".txt"))
    i += 1
    map.foreach(x => writer.write(x.toString()))
    writer.close()

  }


  /**
    * Calculates the Jaccard index between the users input (A) an each movie (B) in the specific cluster.
    *
    * The Jaccard index is given by:
    *
    * J(A, B) = |Intersection(A, B)| / sqrt(|Union(A, B)|)
    *
    * @param movie a Movie object that lies in the same cluster as the users input.
    */
  def jaccardIndex(movie: Movie): Unit = {
    val query = featureArray //User's vector containing Actors, Director etc. as Strings.
    val queryVector = getQueryVector //Gets the query vector.
    val movieVector = movie.getMovieVector() //Movie vector in the same cluster.
    val intersectOfMovies = new Array[Double](query.length)
    var jaccardInd, unionOfMovies: Double = 0

    //Intersect of actors.
    val actorsM1 = Array(query(0), query(1), query(2))
    val actorsM2 = movie.getCast()
    val intersectOfActors = actorsM1.intersect(actorsM2)
    if (intersectOfActors.length >= 1) intersectOfMovies(0) = upperBoundary(0) * (castMap(intersectOfActors(0)) / maxActor)
    if (intersectOfActors.length >= 2) intersectOfMovies(1) = upperBoundary(0) * (castMap(intersectOfActors(1)) / maxActor)
    if (intersectOfActors.length == 3) intersectOfMovies(2) = upperBoundary(0) * (castMap(intersectOfActors(2)) / maxActor)


    //Intersect of directors.
    if (query(3).equals(movie.getDirector())) intersectOfMovies(3) = queryVector(3)

    //Intersect of decade.
    val year = query(4)
    val decMovie1 = year.dropRight(1)
    val decMovie2 = movie.getYear().dropRight(1)
    if (decMovie1.equals(decMovie2)) intersectOfMovies(4) = queryVector(4)

    val genresM1 = Array(query(5), query(6))
    val genresM2 = movie.getGenre()
    if (genresM1(0).equals(genresM2(0)) && genresM1(1).equals(genresM2(1))) {
      intersectOfMovies(5) = queryVector(5)
      intersectOfMovies(6) = queryVector(6)
    }


    //Intersect of Rating
    val rating = query(7)
    if ((rating.toList(0)).equals(movie.getimdbRating().toList(0))) {
      val minRating = math.min(queryVector(7), movieVector(7))
      intersectOfMovies(7) = minRating
    }


    //Intersect of Votes
    val votes = query(8)
    if ((votes.toList.length).equals(movie.getimdbVotes().toList.length)) {
      val minVotes = math.min(queryVector(8), movieVector(8))
      intersectOfMovies(8) = minVotes
    }
    //Intersect of language.
    if (query(9).equals(movie.getLanguage())) intersectOfMovies(9) = queryVector(9)

    //Calculate |Intersection(A, B)|
    val lengthOfIntOfMovie = getVectorLength(intersectOfMovies)

    //Calculate the union of the movies
    unionOfMovies = getVectorLength(queryVector) + getVectorLength(movieVector) - lengthOfIntOfMovie


    //Calculate the Jaccard Index.
    jaccardInd = lengthOfIntOfMovie / unionOfMovies

    //Set the Jaccard Index for the movie.
    movie.setJaccardIndex(jaccardInd)
  }

  /**
    * Calculates the cosine similarity between query (a vector) and each movie vector in the same cluster.
    *
    * Cosine similarity is defined as:
    *
    * Similarity = cos(theta) = (A * B) / (|A|*|B|)
    *
    * where A and B are vectors.
    *
    * @param movie
    */
  def cosineSimilarity(movie: Movie): Unit = {
    //The query containing Actors, Director etc. as Strings.
    val featureVec = featureArray
    //Convert the movie vectors to unit scale.
    val movieVec1 = Vectors.dense(getUnitVector(getQueryVector.toArray))
    val movieVec2 = Vectors.dense(getUnitVector(movie.getMovieVector.toArray)) //Movie vector in the same cluster.

    var similarity: Double = 0
    for (i <- 0 until featureVec.length) {
      similarity += movieVec1(i) * movieVec2(i)
    }
    //Update the cosine similarity of each movie.
    movie.setCosineSimilarity(similarity)
  }


  /**
    * Preforms the calculation of the length of a vector.
    *
    * |v| = sqrt(x_1^2 + x_2^2 + ... + x_n^2)
    *
    *
    * @param array an array containing representing the movie vector.
    * @return the length of that vector.
    */
  def getVectorLength(array: Array[Double]): Double = {
    val lengthOfVector = sqrt((array map {
      pow(_, 2)
    }).sum)
    lengthOfVector
  }

  def getVectorLength(mVector: Vector): Double = {
    val lengthOfVector = Vectors.norm(mVector, 2.0)
    lengthOfVector
  }

  /**
    * Text based menu where the user can choose to get a recommendation based on an existing movie or
    * enter a preferred set of features.
    */
  def recommendationSystem(): Unit = {
    while (true) {
      println("\tWelcome to the \n" +
        "\tK-Means\n #Recommendation system#\n")
      println("1.\tGet recommendations based on a movie." + "\n\n" +
        "2.\tGet recommendations based on your preferences." + "\n\n" +
        "3.\tQuit.\n\n")
      try {
        val userPick = readInt()
        if (userPick == 1) {
          pickAMovie()
        }
        else if (userPick == 2) {
          enterFeatures()
        }
        else if (userPick == 3) {
          return
        }
      }
      catch {
        case e: Exception => println("Invalid input.")
      }
    }
  }


  /**
    * Takes the a query with movie features (Actor, Actor, Actor, Director, Year,
    * Genre, Genre, IMDb-Rating, IMDb-votes, Country) an converts them into a vector.
    * Returns the recommendations by calculating the cluster ID of the vector, and then
    * running the getRecommendations method.
    *
    */
  def enterFeatures(): Unit = {
    val query = readLine("\n\nEnter the features you like accordingly:\n" +
      "Actor #1, Actor #2, Actor #3, Director, Year, Genre #1, Genre #2, IMDb-Rating, IMDb-votes, Language\n")
    featureArray = query.split(",").map(_.trim)
    if (featureArray.length == 10) {
      val predVector = getQueryVector()

      val predictedCluster = clusters.predict(predVector)

      getRecommendations(predictedCluster)
    }
  }

  def getQueryVector(): Vector = {
    val queryArray = Array(castMap(featureArray(0)), castMap(featureArray(1)),
      castMap(featureArray(2)), directorMap(featureArray(3)), yearMap(featureArray(4)),
      genreMap(featureArray(5)), genreMap(featureArray(6)),
      featureArray(7).toDouble, featureArray(8).toDouble,
      languageMap(featureArray(9)))
    val queryVector = Vectors.dense(minmaxNorm(queryArray))
    queryVector
  }

  def getSelectedMovieArray(selectedMovie: Movie): Array[String] = {
    val queryArray = selectedMovie.getCast() ++ Array(selectedMovie.getDirector(), selectedMovie.getYear()) ++
      selectedMovie.getGenre() ++ Array(selectedMovie.getimdbRating(), selectedMovie.getimdbVotes(),
      selectedMovie.getLanguage())
    queryArray
  }

  /**
    * If the user would like to get a recommendation base on a movie, this method is run. The user's input
    * is sent to the getSuggestion method, which will return all movies containing the words the user has typed
    * in. If the movie does not exist the program will notify the user and return to the main menu.
    */
  def pickAMovie(): Unit = {
    val userInput = readLine("\n\nEnter a name of a movie:")
    getSuggestion(userInput)
  }

  /**
    * Takes a query as argument, checks if it is valid and the if the movie exists.
    * If there are several movies containing the query they will be presented as a list, where
    * the user can select the right movie by entering a number.
    *
    * @param query a movie the user want to base the recommendation on.
    */
  def getSuggestion(query: String): Unit = {
    var movieSuggestions = new ListBuffer[Movie]()
    val pattern = query.r
    for (movie <- movieArray2) {
      if (movie.getTitle().contains(query)) {
        movieSuggestions += movie
      }
    }

    if (movieSuggestions.length == 0) {
      println("The movie does not exist.\n\n")
      return
    }

    var k = 1
    for (movie <- movieSuggestions) {
      println(k + ". " + movie.getTitle + "\t(" + movie.getYear + ")\t" + movie.getimdbRating + "/10" + " (" + movie.getimdbVotes + ")" + "\n")
      k += 1
    }
    println("\nPress '0' to return to main menu.\n")

    while (true) {
      try {
        val userPick = readInt()
        if (userPick - 1 >= 0 && userPick - 1 <= k) {
          val query = movieSuggestions(userPick - 1)
          featureArray = getSelectedMovieArray(query)
          println(featureArray.mkString(" "))
          val predictedCluster = clusters.predict(query.getMovieVector)
          println(query.getMovieVector)
          getRecommendations(query, predictedCluster)
          return
        }

        else if (userPick == 0) {
          return
        }

        else {
          println("Invalid input.")
        }
      }
      catch {
        case e: Exception => println("Invalid input.")
      }
    }
  }


  /**
    * Gets the recommendations based on a movie.
    *
    * Sorts the movie set based on the cluster ID belonging to the user's input. Calculates the Jaccard index
    * and the Cosine similarity of each movie. Then sorts the movies based on the one of these similarities.
    *
    * Will get the 5 most relevant movies based on the query.
    *
    * @param predictedCluster the cluster ID of the user's input.
    * @param query            the movie queried.
    */
  def getRecommendations(query: Movie, predictedCluster: Int): Unit = {
    val moviesInCluster = sc.parallelize(movieArray2, 2).filter(m => m.getClusterID() == predictedCluster)

    movieArray3 = moviesInCluster.collect()
    movieArray3.foreach(jaccardIndex)
    movieArray3.foreach(cosineSimilarity)

    println(moviesInCluster.count)

    //To prevent that the same movie that was query will be recommended, the movie ID is compared with the query.
    val recommendations = sc.parallelize(movieArray3, 2)
      .takeOrdered(6)(Ordering[Double].reverse.on(m => (m.getJaccardIndex)))
    recommendations.foreach(m => if (m.getID != query.getID) println(m.printMovie()))

  }


  /**
    * Gets the recommendations based on a preferences.
    *
    * Sorts the movie set based on the cluster ID belonging to the user's input. Calculates the Jaccard index
    * and the Cosine similarity of each movie. Then sorts the movies based on the one of these similarities.
    *
    * Will get the 5 most relevant movies based on the query.
    *
    * @param predictedCluster the cluster ID of the user's input.
    */
  def getRecommendations(predictedCluster: Int): Unit = {
    val moviesInCluster = sc.parallelize(movieArray2, 2).filter(m => m.getClusterID() == predictedCluster)

    movieArray3 = moviesInCluster.collect()
    movieArray3.foreach(jaccardIndex)
    movieArray3.foreach(cosineSimilarity)

    //If the movie is based on features the five most similar movies will be displayed.
    val recommendations = sc.parallelize(movieArray3, 2)
      .takeOrdered(5)(Ordering[Double].reverse.on(m => (m.getJaccardIndex)))
    recommendations.foreach(m => println(m.printMovie()))
  }


  /**
    * Reads the text file, sorts it, calculates the centroids using K-Means. Then calculates the which cluster
    * each movie belongs to.
    *
    */
  def runKMeans(): Unit = {
    val iso88591 = "iso-8859-1"
    var sortedDatabase = new ListBuffer[String]()
    checkForDuplicate = new ListBuffer[(String, String, String)]

    var i = false
    for (line <- Source.fromFile(pathToDatabaseMac, iso88591).getLines) {
      if (i && sortWithIMDBStats(line)) {
        sortedDatabase += line
      }
      i = true
    }

    checkForDuplicate = null

    sortedDatabase.foreach(getMovie)

    for (movie <- movieArray) {
      minmaxNorm(movie)
    }

    println(genreMap)

    trainingData = sc.parallelize(movieArray)

    val parsedData = trainingData.map(s => s.getMovieVector).cache()
    //getElbow(parsedData)

    //Calculate the pos. of the centroids using K-Means clustering.
    val numClusters = 180
    val numIterations = 50
    clusters = KMeans.train(parsedData, numClusters, numIterations, 1, "k-means||")

    trainingData.foreach(predictCluster)
  }


  /**
    * Runs the program.
    *
    * @param args N/A
    */
  def main(args: Array[String]) {
    runKMeans()
    recommendationSystem()
  }
}