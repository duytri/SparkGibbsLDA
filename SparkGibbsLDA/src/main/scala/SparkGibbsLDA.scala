package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkGibbsLDA {
  def main(args: Array[String]): Unit = {
    println("Collapsed Gibbs sampling LDA in Apache Spark.")

    val startTime = System.currentTimeMillis()

    //~~~~~~~~~~~ Spark ~~~~~~~~~~~
    val conf = new SparkConf().setAppName("SparkGibbsLDA").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = Array(1, 2, 3, 4, 5)
    val distData = sc.parallelize(data)
    println("Tong cua mang la: " + distData.reduce(_ + _))
  }
}