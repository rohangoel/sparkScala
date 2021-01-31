import org.apache.spark.{SparkConf,SparkContext}
import org.apache.log4j._

object ratingHistogram 
{ 
  def main(args: Array[String]): Unit = 
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = new SparkConf().setAppName("ratingHistogram").setMaster("local[4]")
    val sc = new SparkContext(spark)
    val in_file = sc.textFile("/mnt/c/Users/rohan/OneDrive/Desktop/SparkScala/SparkScalaCourse/data/ml-100k/u.data")
    val ratings = in_file.map(x => x.split("\t")(2))
    val ratings_cnt = ratings.countByValue()
    val sort_ratings = ratings_cnt.toSeq.sortBy(_._1)

    sort_ratings.foreach(println)
    sc.stop

  }
}
