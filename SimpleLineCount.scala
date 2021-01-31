import org.apache.spark.{SparkContext, SparkConf}

object linecount{
            def main(args : Array[String]): Unit = {

              val SparkConf = new SparkConf().setAppName("App").setMaster("local[*]")

              val sc1 = new SparkContext(SparkConf)

              val lines = sc1.textFile("/mnt/c/Users/rohan/OneDrive/Desktop/SparkScala/SparkScalaCourse/data/ml-100k/u.data")

              val numlines = lines.count

              println(numlines)
              sc1.stop()

                        }
}
