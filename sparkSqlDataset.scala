import org.apache.spark.sql._
import org.apache.log4j._

object sparkDatasets {

  case class Person(id:Int,name:String,Age:Int,Friends:Int)

def main(args:Array[String])
{

  val spark = SparkSession.builder.appName("sparkDatasets").master("local[*]").getOrCreate()
  
  import spark.implicits._
  val schemaPeople = spark.read.option("header","true").option("InferSchema","true").csv("/mnt/c/Users/rohan/OneDrive/Desktop/SparkScala/SparkScalaCourse/data/fakefriends.csv").as[Person]
  schemaPeople.printSchema()
  schemaPeople.createOrReplaceTempView("People")
  val teenagers = spark.sql("select * from People where age >= 13 and age <= 19")
  teenagers.show()
  //val results = teenagers.collect()
  //results.foreach(println)
}

}
