import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType,FloatType,StringType,LongType,StructType}
import org.apache.spark.sql.functions._
import org.apache.log4j._

object popularMovies{
 case class Movie(movieId:Int)
 def main(args:Array[String])
 {
   Logger.getLogger("Org").setLevel(Level.ERROR)
   val spark = SparkSession.builder.appName("popularMovies").master("local[*]").getOrCreate()
   import spark.implicits._
   val movieSchema = new StructType().add("userId",IntegerType).add("movieId",IntegerType).add("rating",IntegerType).add("timeStamp",LongType)
   val in_movies = spark.read.schema(movieSchema).option("sep","\t").csv("/mnt/c/Users/rohan/OneDrive/Desktop/SparkScala/SparkScalaCourse/data/ml-100k/u.data").as[Movie]
   in_movies.show()
   val popularMovies = in_movies.groupBy($"movieId").count().select($"movieId",$"count".as("noOfRatings")).orderBy(desc("noOfRatings"))
   popularMovies.show()
   spark.stop()
 }  

}
