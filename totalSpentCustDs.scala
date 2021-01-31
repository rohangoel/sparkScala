import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType,IntegerType,FloatType,StructType}
import org.apache.log4j._
import org.apache.spark.sql.SparkSession

object totalSpentCustDs {
  case class trans(CustId:Int,TransId:Int,Amnt:Float)
  def main(args:Array[String])
  {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("totalSpentCustDs").master("local[*]").getOrCreate()
    import spark.implicits._
    val transSchema = new StructType().add("CustId",IntegerType,nullable = false).add("TransId",IntegerType,nullable = false).add("Amnt",FloatType,nullable = false)
    val inDs_trans = spark.read.schema(transSchema).csv("/mnt/c/Users/rohan/OneDrive/Desktop/SparkScala/SparkScalaCourse/data/customer-orders.csv").as[trans]
    val inDs_trans_selected = inDs_trans.select($"CustId",$"Amnt")
    val total_spent_cust = inDs_trans_selected.groupBy($"CustId").sum("Amnt").select($"CustId",$"sum(Amnt)".as("TotalAmnt"))
    total_spent_cust.show()
  }

    
}
