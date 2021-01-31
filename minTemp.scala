import org.apache.spark.{SparkConf,SparkContext}
import scala.math


object minTemp {    
  def parselines(line:String):(String,String,Float) = 
  { 
    val fields = line.split(",") 
    val stationId = fields(0)
    val entryType = fields(2)
    val temprature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationId,entryType,temprature)
  }

  def main(args:Array[String])
  {
    val spark = new SparkConf().setAppName("minTemp").setMaster("local[4]")

     val sc = new SparkContext(spark)

     val rdd_weather = sc.textFile("/mnt/c/Users/rohan/OneDrive/Desktop/SparkScala/SparkScalaCourse/data/1800.csv")

     val rdd_weather_1 = rdd_weather.map(parselines)

     val rdd_weather_2  = rdd_weather_1.filter(x => x._2 == "TMIN")

     val rdd_weather_3 = rdd_weather_2.map(x => (x._1,x._3.toFloat))

     val rdd_weather_4 = rdd_weather_3.reduceByKey((x,y) => math.min(x,y))

     val results = rdd_weather_4.collect()

     for(result <- results.sorted) {
       val station = result._1
       val temp = result._2
       val formattedtemp = f"$temp%.2f F"
       println(s"$station minimum temprature is: $formattedtemp")

     }

      sc.stop()


  }   
}
