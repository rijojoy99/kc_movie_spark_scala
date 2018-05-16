

import com.kc.movieratings.MovieInfo
import com.kc.movieratings.MovieInfo.{Movies, Ratings, Users}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object MoviesScalaSpark {

  val log = Logger.getRootLogger

  def main(args: Array[String]): Unit
  = {
    println("Start of Prog")
    log.setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Movie_Ratings").master("local[8]").getOrCreate()
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.sparkContext.hadoopConfiguration.set("textinputformat.record.delimiter", MovieInfo.delim)

    import spark.implicits._
    val movies = MovieInfo.getMovies("data/movies.dat", spark)
    val moviesDF = spark.createDataFrame(movies).as[Movies]
    val moviesKeyBy = movies.keyBy(f => f.movieId)

    val ratings = MovieInfo.getRatings("data/ratings.dat", spark)
    val ratingsDF = spark.createDataFrame(ratings).as[Ratings]
    val ratingsTopMovies = ratings.keyBy(f => f.movieId)

    val users = MovieInfo.getUsers("data/users.dat", spark)
    val usersDF = spark.createDataFrame(users).as[Users]

    /* Top ten most viewed movies with their movies Name (Ascending or Descending order) */

    // Can do it in Datasets even
    //    val moviesJoinedRatings = moviesDF.join(ratingsDF, moviesDF("movieId") <=> ratingsDF("movieId"), "inner")
    //      .select(moviesDF("movieName"), ratingsDF("movieId"))

    val top20MostViewedMovies = ratingsTopMovies.join(moviesKeyBy).map(x => ((x._2._2.movieName), 1))
      .reduceByKey((x, y) => x + y)
      .keyBy(f => f._2)
      .sortByKey(false)
      .take(20)
      .map(x => (x._2._1, x._2._2))

    println("**********TOP 20 Movies by Visits*******************")
    spark.createDataFrame(top20MostViewedMovies).toDF("Movie", "No_Of_Visits").coalesce(1)
      .write.format("com.databricks.spark.csv").option("header",true).mode(SaveMode.Overwrite).csv("output/No_Of_Visits.csv")

    val top20MostRatedMovies = ratingsTopMovies.join(moviesKeyBy)
      .filter(f => f._2._1.userId.nonEmpty || f._2._1.rating.isValidInt)
      .map { case (x, y) => ((y._2.movieName), (y._1.rating.toDouble, 1)) }
      .reduceByKey((x, y) =>
        (x._1 + y._1, x._2 + y._2))
      .filter(f => f._2._2 >= 40)
      .map(x => (x._1, (x._2._1 / x._2._2)))
      .keyBy(k => k._2)
      .sortByKey(false)
      .top(20)
      .map(x => (x._1, x._2._1))

    println("**********TOP 20 Movies by Average Rating*******************")
    spark.createDataFrame(top20MostRatedMovies).toDF("Average_Ratting", "Movie").coalesce(1)
      .write.format("com.databricks.spark.csv").option("header",true).mode(SaveMode.Overwrite).csv("output/Average_Ratting.csv")

    /*We wish to know how have the genres ranked by Average Rating, for each profession and age
      group. The age groups to be considered are: 18-35, 36-50 and 50+*/

    //Fetch the Dataset with genres as rows for each
    val moviesExp = moviesDF.select('movieId, 'movieName, split('genres, "\\|").as("genres"))
      .withColumn("genre", explode('genres))
      .drop("genres")

    val userWithGenreRating = moviesExp.join(ratingsDF, moviesExp("movieId") === ratingsDF("movieId"), "inner")
      .select(ratingsDF("userId"), ratingsDF("rating"), moviesExp("genre"))

    val userJoinUsrwthGRating = usersDF.join(userWithGenreRating, usersDF("userId") === userWithGenreRating("userId"), "inner")
      .select('occupation, 'genre, 'age, 'rating)
      .rdd
      .map(x => {
        val ageVal = x.getAs[Int]("age")
        val ageGrp = ageVal match {
          case y if (y >= 18 && y <= 35) => "18-35"
          case y if (y >= 36 && y <= 50) => "36-50"
          case y if (y > 50) => "50+"
          case y if y < 18 => "Under 18"
        }
        val occuptn = x.getAs[String]("occupation") match {
          case y if y == "0" => "other"
          case y if y == "1" => "academic/educator"
          case y if y == "2" => "artist"
          case y if y == "3" => "clerical/admin"
          case y if y == "4" => "college/grad student"
          case y if y == "5" => "customer service"
          case y if y == "6" => "doctor/health care"
          case y if y == "7" => "eyecutive/managerial"
          case y if y == "8" => "farmer"
          case y if y == "9" => "homemaker"
          case y if y == "10" => "K-12 student"
          case y if y == "11" => "lawyer"
          case y if y == "12" => "programmer"
          case y if y == "13" => "retired"
          case y if y == "14" => "sales/marketing"
          case y if y == "15" => "scientist"
          case y if y == "16" => "self-employed"
          case y if y == "17" => "technician/engineer"
          case y if y == "18" => "tradesman/craftsman"
          case y if y == "19" => "unemployed"
          case y if y == "20" => "writer"
          case _ => "unknown"
        }
        (occuptn, ageGrp, x.getAs[Int]("rating").toDouble, x.getAs[String]("genre"))
      })
      .toDF("Occupation", "AgeGroup", "rating", "genre")
      .dropDuplicates()
      .repartition(16, col("Occupation"), col("AgeGroup"), col("genre"))
      .withColumn("avgVal", avg(col("rating").cast("Double")).over(Window.partitionBy("Occupation", "AgeGroup", "genre")))
      .drop("rating")
      .distinct()
      .withColumn("rankVal", row_number().over(Window.partitionBy("Occupation", "AgeGroup").orderBy(col("avgVal").desc)))
      .filter(col("rankVal") <= lit(5))
      .drop("avgVal")
      .select(col("Occupation"), col("AgeGroup"), col("genre"), col("rankVal"))
      .groupBy(col("Occupation"), col("AgeGroup"))
      .pivot("rankVal")
      .agg(collect_list("genre"))
      .withColumn("Rank1", $"1"(0))
      .withColumn("Rank2", $"2"(0))
      .withColumn("Rank3", $"3"(0))
      .withColumn("Rank4", $"4"(0))
      .withColumn("Rank5", $"5"(0))
      .drop("1","2","3","4","5")
      .orderBy("Occupation","AgeGroup")

    userJoinUsrwthGRating.show(10, false)
    userJoinUsrwthGRating.coalesce(1).write.format("com.databricks.spark.csv").option("header",true).mode(SaveMode.Overwrite).csv("output/kc_movie_rating_report.csv")

    sys.exit(0)
/*
 // Replication using RDDs
    val userJoinusrwthGRatingTmp = usersDF.join(userWithGenreRating, usersDF("userId") === userWithGenreRating("userId"), "inner")
      .select('occupation, 'genre, 'age, 'rating)
      .rdd
      .map(x => {
        val ageVal = x.getAs[Int]("age")
        val ageGrp = ageVal match {
          case y if (y >= 18 && y <= 35) => "18-35"
          case y if (y >= 36 && y <= 50) => "36-50"
          case y if (y > 50) => "50+"
          case y if y < 18 => "Under 18"
        }
        val occuptn = x.getAs[String]("occupation") match {
          case y if y == "0" => "other"
          case y if y == "1" => "academic/educator"
          case y if y == "2" => "artist"
          case y if y == "3" => "clerical/admin"
          case y if y == "4" => "college/grad student"
          case y if y == "5" => "customer service"
          case y if y == "6" => "doctor/health care"
          case y if y == "7" => "eyecutive/managerial"
          case y if y == "8" => "farmer"
          case y if y == "9" => "homemaker"
          case y if y == "10" => "K-12 student"
          case y if y == "11" => "lawyer"
          case y if y == "12" => "programmer"
          case y if y == "13" => "retired"
          case y if y == "14" => "sales/marketing"
          case y if y == "15" => "scientist"
          case y if y == "16" => "self-employed"
          case y if y == "17" => "technician/engineer"
          case y if y == "18" => "tradesman/craftsman"
          case y if y == "19" => "unemployed"
          case y if y == "20" => "writer"
          case _ => "unknown"
        }
        (occuptn, ageGrp, x.getAs[Int]("rating").toDouble, x.getAs[String]("genre"))
      })
      .map { case (x) => ((x._1, x._2, x._4), (x._3, 1)) }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      //      .keyBy(k => k._1)
      //      .sortBy(s => s._2)
      .topByKey(5)
      .map(x => ((x._1._1, x._1._2, x._1._3) -> x._2.map(k => k._1 / k._2).toList(0)))
    //      .toDF("Occupation", "AgeGroup", "Genre", "AverageRating")

    userJoinusrwthGRatingTmp.take(20).foreach(println)*/
  }
}
