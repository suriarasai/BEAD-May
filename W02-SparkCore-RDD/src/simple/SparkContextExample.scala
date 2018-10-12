package simple

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkContextExample {
  def main(args: Array[String]) {
    val stocksPath = "hdfs://quickstart.cloudera/user/cloudera/stocks.txt"
    val conf = new SparkConf().setAppName("Counting Lines").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.textFile(stocksPath, 2)
    val totalLines = data.count()
    println("Total number of Lines: %s".format(totalLines))
  }
}