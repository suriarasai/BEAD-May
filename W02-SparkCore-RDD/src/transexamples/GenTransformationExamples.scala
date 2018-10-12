package transexamples

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object GenTransformationExamples {

  /*Examples of general transformation functions are:
•	map
•	filter
•	flatMap
•	groupByKey
•	sortByKey
•	combineByKey*/

  Logger.getLogger("org").setLevel(Level.OFF)
  val conf = new SparkConf().setAppName("Parallelize Example").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val rdd_one = sc.parallelize(Seq(1, 2, 3, 4, 5, 6))
  rdd_one.count
  rdd_one.cache
  rdd_one.count

  val rdd_two = sc.textFile("wiki1.txt")
  rdd_two.count
  rdd_two.first
  val rdd_three = rdd_two.map(line => line.split(" "))

  rdd_three.take(1)
  val rdd_four = rdd_two.flatMap(line => line.split(" "))
  rdd_four.take(10)

  val rdd_five = rdd_two.filter(line => line.contains("Spark"))
  rdd_five.count
  
  
  
  
}