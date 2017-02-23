package org.BachelorThesis

import org.apache.spark.mllib.linalg.Vector

import scala.collection.mutable.ListBuffer

/**
  * Created by Gustaf on 2016-03-26.
  *
  * ID
  * imdbID
  * Title
  * Year
  * Rating
  * Runtime
  * Genre
  * Released
  * Director
  * Writer
  * Cast
  * Metacritic
  * imdbRating
  * imdbVotes
  * Poster
  * Plot
  * FullPlot
  * Language
  * Country
  * Awards
  * lastUpdated
  *
  *
  */


class Movie(movie: String) extends Serializable {
  val attributesList = movie.split("\t")

  val ID = attributesList(0)
  val imdbID = attributesList(1)
  val Title = attributesList(2)
  val Year = attributesList(3)
  val Rating = attributesList(4)
  val Runtime = attributesList(5)
  var Genre: Array[String] = null
  setGenre()
  val Released = attributesList(7).split(",")(0)
  var Director: String = null
  //val Director = attributesList(8).split(",")(0)
  setDirector()
  val Writer = attributesList(9).split(",")(0)
  var Cast: Array[String] = null
  setCast()
  val Metacritic = attributesList(11)
  val imdbRating = attributesList(12)
  val imdbVotes = attributesList(13)
  val Plot = attributesList(15)
  val FullPlot = attributesList(16)
  val Language = attributesList(17).split(",")(0)
  val Country = attributesList(18).split(",")(0)
  val Awards = attributesList(19)
  var movieVector: Vector = null
  var ClusterID: Int = 0
  var JaccardIndex: Double = 0
  var CosineSimilarity: Double = 0

  def setCast(): Array[String] = {
    val tempList = attributesList(10).split(",")
    Cast = new Array[String](3)
    for (i <- 0 until 3) {
      try
        Cast(i) = tempList(i).trim
      catch {
        case iob: IndexOutOfBoundsException => Cast(i) = ""
      }
    }
    Cast
  }

  def setGenre(): Array[String] = {
    val tempList = attributesList(6).split(",")
    Genre = new Array[String](2)
    for (i <- 0 until 2) {
      try
        Genre(i) = tempList(i).trim
      catch {
        case iob: IndexOutOfBoundsException => Genre(i) = ""
      }
    }
    Genre
  }

  def getID(): String = {
    ID
  }

  def getYear(): String = {
    Year
  }

  def getTitle(): String = {
    Title
  }


  def getGenre(): Array[String] = {
    Genre
  }

  def setDirector(): Unit = {
    val tempArray = attributesList(8).split(",")
    var tempList = List[String]()
    for (i <- 0 until tempArray.length) {
      tempList :+= tempArray(i).trim
    }
    Director = tempList.sortWith((x, y) => x.length() < y.length()).mkString(" ")
  }

  def getDirector(): String = {
    Director
  }

  def getWriter(): String = {
    Writer
  }

  def getCast(): Array[String] = {
    Cast
  }

  def getimdbRating(): String = {
    imdbRating
  }

  def getimdbVotes(): String = {
    imdbVotes
  }

  def getPlot(): String = {
    var i = 0
    var tempArray = new ListBuffer[String]()
    for (str <- Plot.split(" ")) {
      if (i == 10) {
        tempArray += "\n" + str
        i = 0
      }
      else tempArray += str
      i += 1
    }
    val formattedPlot = tempArray.mkString(" ")
    formattedPlot
  }

  def getFullPlot(): String = {
    FullPlot
  }

  def getLanguage(): String = {
    Language
  }

  def getCountry(): String = {
    Country
  }

  def getAwards(): String = {
    Awards
  }

  def setMovieVector(vector: Vector): Unit = {
    movieVector = vector
  }

  def getMovieVector(): Vector = {
    movieVector
  }

  def setClusterID(clusterID: Int): Unit = {
    ClusterID = clusterID
  }

  def getClusterID(): Int = {
    ClusterID
  }

  def setJaccardIndex(jI: Double): Unit = {
    JaccardIndex = jI
  }

  def getJaccardIndex(): Double = {
    JaccardIndex
  }

  def setCosineSimilarity(cs: Double): Unit = {
    CosineSimilarity = cs
  }

  def getCosineSimilarity(): Double = {
    CosineSimilarity
  }


  def printMovie(): String = {
    val fullMovie = getTitle + "\t(" + getYear + ")\t" + getimdbRating + "/10" + " (" + getimdbVotes + ")" + "\n" +
      Rating + " | " + Runtime + " | " + getGenre.mkString(", ") + " | " + Released + " \n" +
      "Plot:\n" + getPlot() + "\n" +
      "Director: " + getDirector + "\n" +
      "Writers: " + getWriter + "\n" +
      "Cast:\n" + getCast.mkString("\n") + "\n" +
      "Country: " + getCountry() + "\n" +
      "Jaccard index: " + "%1.4f".format(getJaccardIndex) + "\n" +
      "Cosine similarity: " + "%1.4f".format(getCosineSimilarity) + "\n\n" +
      getMovieVector +"\n"
    fullMovie.toString
  }

}

