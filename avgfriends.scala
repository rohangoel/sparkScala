import org.apache.spark.{SparkConf,SparkContext}
import org.apache.log4j._

object FrnsByAge {
  def parseline(line:String):(Int,Int) = 
  {
    val age = line.split(",")(2).toInt
    val frns = line.split(",")(3).toInt
    (age,frns)
  }

  def main(args:Array[String])
  {
     Logger.getLogger("org").setLevel(Level.ERROR)
     val spark = new SparkConf().setAppName("frnsAvg").setMaster("local[4]")
     val sc = new SparkContext(spark)
     val readFile_rdd = sc.textFile("/mnt/c/Users/rohan/OneDrive/Desktop/SparkScala/SparkScalaCourse/data/fakefriends-noheader.csv")
     val ageFrns_rdd = readFile_rdd.map(parseline)
    // val ageFrns_rdd = readFile_rdd.map(x => (x.split(",")(2).toInt,x.split(",")(3).toInt))
     val totalbyAge = ageFrns_rdd.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
     val averagesbyAge = totalbyAge.mapValues(x => x._1/x._2)
     val results = averagesbyAge.collect()
     println("it executed till here" + results)
    // results.sorted.foreach(println)
    //results.coalesce(1).saveAsTextFile("abc1.dat") 
    println("file is written")
     sc.stop()
  }
}  
