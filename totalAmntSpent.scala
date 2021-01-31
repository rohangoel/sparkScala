import org.apache.spark.{SparkContext,SparkConf}
import org.apache.log4j._


object TotalAmountSpentRohan {
    def parselines(line:String):(Int,Float) =
          {
                  val fields = line.split(",")
                        val custId = fields(0).toInt
                              val amntSpnt = fields(2).toFloat
                                    (custId,amntSpnt)
                                        }
      def main(args:Array[String]): Unit =
          {
                Logger.getLogger("org").setLevel(Level.ERROR)
                    val spark = new SparkConf().setAppName("TotalAmountSpent").setMaster("local[*]")
                        val sc = new SparkContext(spark)
                            val readFile = sc.textFile("/mnt/c/Users/rohan/OneDrive/Desktop/SparkScala/SparkScalaCourse/data/customer-orders.csv")
                                val amntDtls = readFile.map(parselines)
                                    val totalByCust = amntDtls.reduceByKey((x,y) => (x+y))
                                        val totalByCust_formatted = totalByCust.mapValues(x => x.toInt)
                                            val results = totalByCust_formatted.collect()
                                                results.sortBy(-_._2).foreach(println)
                                                    sc.stop()

                                                      }
}
