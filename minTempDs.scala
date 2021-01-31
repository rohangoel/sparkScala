import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType,StructType,IntegerType,FloatType}
import org.apache.spark.sql.SparkSession
import org.apache.log4j._

object minTempDs {
  case class Temprature(StationId:String,date:Int,measure_type:String,temprature:Float)
  def main(args:Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("minTempDs").master("local[*]").getOrCreate()
    val tempSchema = new StructType().add("StationId" ,StringType,nullable = true).add("date",IntegerType,nullable = true).add("measure_type",StringType,nullable = true).add("temprature",FloatType,nullable = true)
    import spark.implicits._
    val inDs = spark.read.schema(tempSchema).csv("/mnt/c/Users/rohan/OneDrive/Desktop/SparkScala/SparkScalaCourse/data/1800.csv").as[Temprature]
    val inDs_min_only = inDs.filter($"measure_type" === "TMIN")
    val inDs_req_fields_only = inDs_min_only.select($"StationId",$"temprature")
    val inDs_min_temp = inDs_req_fields_only.groupBy($"StationId").min("temprature")
    inDs_min_temp.show()
    val inDs_min_temp_final = inDs_min_temp.withColumn("temp_min",round($"min(temprature)"* 0.1f * (9.0f / 5.0f) + 32.0f, 2)).orderBy($"temp_min")
    inDs_min_temp_final.show()
    val results = inDs_min_temp_final.collect()
  }
    
}
