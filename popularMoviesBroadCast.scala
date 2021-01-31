import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,udf}
import org.apache.spark.sql.types.{StructType,IntegerType,LongType}

import scala.io.{Codec,Source}

object popularMoviesBroadCast{
  case class movies(userId:Int,movieId:Int,rating:Int,timeStamp:Int)
  def loadMovieNames():Map[Int,String] = 
  {
    implicit val codec: Codec = Codec("ISO-8859-1")
    var movieName:Map[Int,String] = Map()
    val lines = Source.fromFile("/mnt/c/Users/rohan/OneDrive/Desktop/SparkScala/SparkScalaCourse/data/ml-100k/u.item")
    for(line <- lines.getLines())
    {
      val fields = line.split("\\|")
      if(fields.length > 1)
      {
        movieName += (fields(0).toInt -> fields(1))
      }
    }
    lines.close()
    movieName
  }
def main(args:Array[String])
{ 
  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder.appName("popularMoviesBroadCast").master("local[*]").getOrCreate()
  
  val nameDict = spark.sparkContext.broadcast(loadMovieNames())

  import spark.implicits._

  val movieSchema = new StructType().add("userId",IntegerType,nullable = true).add("movieId",IntegerType,nullable = true).add("rating",IntegerType,nullable = true).add("timeStamp",IntegerType,nullable = true)

  val movieRatingDS = spark.read.option("sep","\t").schema(movieSchema).csv("/mnt/c/Users/rohan/OneDrive/Desktop/SparkScala/SparkScalaCourse/data/ml-100k/u.data").as[movies] 

  val mostPopularDF = movieRatingDS.groupBy($"movieId").count().select($"movieId",$"count".as("noOfRatings"))

  //define an anonmyous function instead of regular function

  val lookupName : Int => String = (movieId:Int) => {nameDict.value(movieId)}
  
  // regular function expression
  //def lookupName(movieId:Int):String = {nameDict.value(arg1)}

  val lookupValue = udf(lookupName)

  val moviesWithNames = mostPopularDF.withColumn("movieTitle",lookupValue($"movieId"))

 // val moviesWithNamesSorted = moviesWithNames.orderBy(desc("noOfRatings"))

  moviesWithNames.show(false)
}





}

