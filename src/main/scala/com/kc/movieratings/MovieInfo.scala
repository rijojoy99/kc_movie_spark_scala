package com.kc.movieratings

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object MovieInfo {
  val delim: String = "\\:\\:"

  def getMovies(path: String, spark: SparkSession): RDD[Movies] = {

    println(s"Getting Data from the path => $path")
    val tmp = spark.read.option("inferschema", true).option("delimiter", delim).textFile(path)
      .rdd
      .map(_.replaceAll("\\:\\:", "\t").split("\t").toSeq)
      .map(y => Movies(y(0).trim, y(1).trim, y(2).trim))
    tmp
  }

  def getUsers(path: String, spark: SparkSession): RDD[Users] = {

    println(s"Getting Data from the path => $path")
    val tmp = spark.read.option("inferschema", true).option("delimiter", delim).textFile(path)
      .rdd
      .map(_.replaceAll("\\:\\:", "\t").split("\t").toSeq)
      .map(y => Users(y(0).trim, y(1).trim, y(2).trim.toInt, y(3).trim, y(4).trim))
    tmp
  }

  def getRatings(path: String, spark: SparkSession): RDD[Ratings] = {

    println(s"Getting Data from the path => $path")
    val tmp = spark.read.option("inferschema", true).option("delimiter", delim).textFile(path)
      .rdd
      .map(_.replaceAll("\\:\\:", "\t").split("\t").toSeq)
      .map(y => Ratings(y(0).trim, y(1).trim, y(2).trim.toInt, y(3).trim))
    tmp
  }

  case class Movies(movieId: String, movieName: String, genres: String)

  case class Ratings(userId: String, movieId: String, rating: Int, time: String)

  case class Users(userId: String, gender: String, age: Int, occupation: String, zipcode: String)

}
