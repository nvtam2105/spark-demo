/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SimpleApp {
  def main(args: Array[String]) {
    // val logFile = "README.md" // Should be some file on your system
    // val conf = new SparkConf()
    // .setAppName("Simple Application")
    // .setMaster("local[4]")
    // val sc = new SparkContext(conf)
    // val logData = sc.textFile(logFile, 2).cache()

    // val numAs = logData.filter(line => line.contains("a")).count()
    // val numBs = logData.filter(line => line.contains("b")).count()
    // println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

    val spark = SparkSession
		.builder()
		.appName("Simple Application")
		.config("spark.some.config.option", "some-value")
		.getOrCreate()

	// For implicit conversions like converting RDDs to DataFrames
	import spark.implicits._

	val df = spark.read
		.format("csv")
		.option("header", "true")
		.option("inferSchema", "true")
		.option("delimiter", ";")
		.load("bank/bank-full.csv")
	
	df.coalesce(1).write.format("csv").option("header", "true").save("bank-full")
	//df.printSchema()


	 df.filter("housing = 'yes' and loan = 'yes'")
		.groupBy("age").count()
		.sort($"count" desc)
		.limit(10).coalesce(1).write.format("csv").option("header", "true").save("q1")


	df.filter("poutcome = 'success'")
		.groupBy("job").count()
		.sort($"count" desc)
		.limit(10).coalesce(1).write.format("csv").option("header", "true").save("q2")

	df.filter("marital = 'married'")
		.groupBy("age").count()
		.sort($"count" desc).coalesce(1).write.format("csv").option("header", "true").save("q3")
  }
}