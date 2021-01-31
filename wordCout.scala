import org.apache.spark.{SparkConf,SparkContext}
import org.apache.log4j._

object wordCount{

  def main(args:Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR) 
    val spark = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc = new SparkContext(spark)
    val readFile = sc.textFile("/mnt/c/Users/rohan/OneDrive/Desktop/SparkScala/SparkScalaCourse/data/book.txt")
    val allWords = readFile.flatMap(x => x.split(" "))
    val wordCount = allWords.countByValue()
   // val result = wordCount.collect()
    println("the script has executed till here")
  //  wordCount.foreach(println)
  }
    

}
